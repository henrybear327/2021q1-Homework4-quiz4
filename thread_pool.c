#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

#include "thread_pool.h"

enum __future_flags {
    __FUTURE_RUNNING = 01,
    __FUTURE_FINISHED = 02,
    __FUTURE_TIMEOUT = 04,
    __FUTURE_CANCELLED = 010, // 8 + __FUTURE_FINISHED
    __FUTURE_DESTROYED = 020, // 16 + __FUTURE_TIMEOUT 
};

typedef struct __threadtask {
    void *(*func)(void *);
    void *arg;
    struct __tpool_future *future;
    struct __threadtask *next;
} threadtask_t;

typedef struct __jobqueue {
    threadtask_t *head, *tail;
    pthread_cond_t cond_nonempty;
    pthread_mutex_t rwlock;
} jobqueue_t;

struct __tpool_future {
    int flag;
    void *result;
    pthread_mutex_t mutex;
    pthread_cond_t cond_finished;
};

struct __threadpool {
    size_t count;
    pthread_t *workers;
    jobqueue_t *jobqueue;
};

static struct __tpool_future *tpool_future_create(void)
{
    struct __tpool_future *future = malloc(sizeof(struct __tpool_future));
    if (future) {
        future->flag = 0;
        future->result = NULL;
        pthread_mutex_init(&future->mutex, NULL);
        pthread_condattr_t attr;
        pthread_condattr_init(&attr);
        pthread_cond_init(&future->cond_finished, &attr);
        pthread_condattr_destroy(&attr);
    }
    return future;
}

int tpool_future_destroy(struct __tpool_future *future)
{
    if (future) {
        pthread_mutex_lock(&future->mutex);
        if (future->flag & __FUTURE_FINISHED || future->flag & __FUTURE_CANCELLED) {
            pthread_mutex_unlock(&future->mutex);
            pthread_mutex_destroy(&future->mutex);
            pthread_cond_destroy(&future->cond_finished);
            free(future);
        } else {
            future->flag |= __FUTURE_DESTROYED;
            pthread_mutex_unlock(&future->mutex);

            // you can't free it, as the task holding this future is still in the queue (not popped when future is destroyed). It's kind of like cancelling the task before it's run.
        }
    }
    return 0;
}

void *tpool_future_get(struct __tpool_future *future, unsigned int seconds)
{
    pthread_mutex_lock(&future->mutex);
    /* turn off the timeout bit set previously */
    future->flag &= ~__FUTURE_TIMEOUT;
    while ((future->flag & __FUTURE_FINISHED) == 0) { // not finished, let's keep waiting
        if (seconds) {
            struct timespec expire_time;
            clock_gettime(CLOCK_MONOTONIC, &expire_time);
            expire_time.tv_sec += seconds;
            int status = pthread_cond_timedwait(&future->cond_finished,
                                                &future->mutex, &expire_time);
            if (status == ETIMEDOUT) {
                future->flag |= __FUTURE_TIMEOUT;
                pthread_mutex_unlock(&future->mutex);
                return NULL;
            }
        } else {
            pthread_cond_wait(&future->cond_finished, &future->mutex); // FFF;
        }
    }

    pthread_mutex_unlock(&future->mutex);
    return future->result;
}

static jobqueue_t *jobqueue_create(void)
{
    jobqueue_t *jobqueue = malloc(sizeof(jobqueue_t));
    if (jobqueue) {
        jobqueue->head = jobqueue->tail = NULL;
        pthread_cond_init(&jobqueue->cond_nonempty, NULL);
        pthread_mutex_init(&jobqueue->rwlock, NULL);
    }
    return jobqueue;
}

static void jobqueue_destroy(jobqueue_t *jobqueue)
{
    threadtask_t *tmp = jobqueue->head;
    while (tmp) {
        jobqueue->head = jobqueue->head->next;
        pthread_mutex_lock(&tmp->future->mutex);
        if (tmp->future->flag & __FUTURE_DESTROYED) {
            pthread_mutex_unlock(&tmp->future->mutex);
            pthread_mutex_destroy(&tmp->future->mutex);
            pthread_cond_destroy(&tmp->future->cond_finished);
            free(tmp->future);
        } else {
            // not destroyed, means that main.c caller are still holding on to the future object!
            // can't free for the caller
            tmp->future->flag |= __FUTURE_CANCELLED;
            pthread_mutex_unlock(&tmp->future->mutex);
        }
        free(tmp);
        tmp = jobqueue->head;
    }

    pthread_mutex_destroy(&jobqueue->rwlock);
    pthread_cond_destroy(&jobqueue->cond_nonempty);
    free(jobqueue);
}

static void __jobqueue_fetch_cleanup(void *arg)
{
    pthread_mutex_t *mutex = (pthread_mutex_t *)arg;
    pthread_mutex_unlock(mutex);
}

// returns the start_routine
static void *jobqueue_fetch(void *queue)
{
    jobqueue_t *jobqueue = (jobqueue_t *)queue;
    threadtask_t *task;
    int old_state; // The previous cancelability type of the thread is returned in the buffer pointed to by oldtype.

    pthread_cleanup_push(__jobqueue_fetch_cleanup, (void *)&jobqueue->rwlock); // kind of like defer -> unlocks the rwlock

    while (1) {
        pthread_mutex_lock(&jobqueue->rwlock); // defer guaranteed this will be done
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_state); 
        pthread_testcancel(); // Calling  pthread_testcancel()  creates  a  cancellation point within the calling thread, so that a thread that is otherwise executing code that contains no  cancellation  points  will respond to a cancellation request. -> test for cancellation here!

        // should be waiting for a job to show up
        while (!jobqueue->tail) // check tail as we pop from tail
            /*
            - rwlock should have been locked at this point, as the while loop might be skipped if there are things in the queue already
            - since we did the rwlock, we guarantee the queue will only be accessed by one thread at a time
            - the pthread_cond_wait will block, waiting for a broadcast from cond_non_empty
            - during the wait, rwlock will be released (unlocked)
            - when the broadcast comes, it's a fight over the rwlock 
            - pthread_cond_wait will always needs to be guarded by a while(boolean expression) since it's blocking here for a condition to happen!
            */
            pthread_cond_wait(&jobqueue->cond_nonempty, &jobqueue->rwlock); // GGG; WA
        
        // we were waiting, so we allow the thread to be cancelled
        // now we are about to execute code, so we don't like the thread to be cancelled anymore
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_state);
        if (jobqueue->head == jobqueue->tail) { // only one task in queue, we empty the queue
            task = jobqueue->tail;
            jobqueue->head = jobqueue->tail = NULL;
        } else { // we pop the tail
            threadtask_t *tmp;
            for (tmp = jobqueue->head; tmp->next != jobqueue->tail; tmp = tmp->next)
                ; // get prev
            task = tmp->next;
            tmp->next = NULL;
            jobqueue->tail = tmp;
        }
        pthread_mutex_unlock(&jobqueue->rwlock);

        if (task->func) {
            pthread_mutex_lock(&task->future->mutex);
            if (task->future->flag & __FUTURE_CANCELLED) { // jobqueue destroyed
                pthread_mutex_unlock(&task->future->mutex);
                free(task);
                continue; // ?? why is this continue? The job queue is already destroyed, waiting at while will be invalid pointer access, no?
            } else {
                task->future->flag |= __FUTURE_RUNNING;
                pthread_mutex_unlock(&task->future->mutex);
            }

            void *ret_value = task->func(task->arg); // ?? we might be executing when future is already destroyed, right?
            pthread_mutex_lock(&task->future->mutex);
            if (task->future->flag & __FUTURE_DESTROYED) {
                pthread_mutex_unlock(&task->future->mutex);
                pthread_mutex_destroy(&task->future->mutex);
                pthread_cond_destroy(&task->future->cond_finished);
                free(task->future);
            } else {
                task->future->flag |= __FUTURE_FINISHED; // KKK
                task->future->result = ret_value;
                pthread_cond_broadcast(&task->future->cond_finished); // LLL, so the tpool_get_future can return the value properly!
                pthread_mutex_unlock(&task->future->mutex);

                // future is where the return value is held, also by main.c caller
                // task encapsulated future
                // so we free task, but not free future, so the caller can get the result!
            }
            free(task);
        } else { // nothing to execute, tpool_join operation will send in no func and arg
            pthread_mutex_destroy(&task->future->mutex);
            pthread_cond_destroy(&task->future->cond_finished);
            free(task->future);
            free(task);
            break; // let's end the thread!
        }
    }

    pthread_cleanup_pop(0); // execute the defer function
    pthread_exit(NULL);
    /*
    The  pthread_exit()  function  terminates the calling thread and returns a value via retval that (if the thread is joinable) is available to another thread in the  same  process  that calls pthread_join(3).

    Any clean-up handlers established by pthread_cleanup_push(3) that have not yet been popped, re popped (in the reverse of the order in which they were pushed) and  executed.   If  the thread  has any thread-specific data, then, after the clean-up handlers have been executed, the corresponding destructor functions are called, in an unspecified order.
    */
}

struct __threadpool *tpool_create(size_t count)
{
    jobqueue_t *jobqueue = jobqueue_create();
    struct __threadpool *pool = malloc(sizeof(struct __threadpool));
    if (!jobqueue || !pool) { // if the creation failed, deinit and return NULL
        if (jobqueue)
            jobqueue_destroy(jobqueue);
        free(pool); // If ptr is NULL, no operation is performed.
        return NULL;
    }

    pool->count = count, pool->jobqueue = jobqueue;
    if ((pool->workers = malloc(count * sizeof(pthread_t)))) {
        for (size_t i = 0; i < count; i++) {
            if (pthread_create(&pool->workers[i], NULL, jobqueue_fetch,
                               (void *)jobqueue)) { // if non-0 == failed
                for (size_t j = 0; j < i; j++)
                    pthread_cancel(pool->workers[j]);
                for (size_t j = 0; j < i; j++)
                    pthread_join(pool->workers[j], NULL);
                free(pool->workers);
                jobqueue_destroy(jobqueue);
                free(pool);
                return NULL;
            }
        }
        return pool;
    }

    jobqueue_destroy(jobqueue);
    free(pool);
    return NULL;
}

struct __tpool_future *tpool_apply(struct __threadpool *pool,
                                   void *(*func)(void *), void *arg)
{
    jobqueue_t *jobqueue = pool->jobqueue;
    threadtask_t *new_head = malloc(sizeof(threadtask_t));
    struct __tpool_future *future = tpool_future_create();
    if (new_head && future) {
        new_head->func = func, new_head->arg = arg, new_head->future = future;
        pthread_mutex_lock(&jobqueue->rwlock);
        if (jobqueue->head) { // has head already
            new_head->next = jobqueue->head;
            jobqueue->head = new_head;
        } else { // empty queue
            jobqueue->head = jobqueue->tail = new_head;
            pthread_cond_broadcast(&jobqueue->cond_nonempty); // HHH;
        }
        pthread_mutex_unlock(&jobqueue->rwlock);
    } else if (new_head) { // future creation failed
        free(new_head);
        return NULL;
    } else if (future) { // new_head malloc failed
        tpool_future_destroy(future);
        return NULL;
    }
    return future;
}

int tpool_join(struct __threadpool *pool)
{
    size_t num_threads = pool->count;
    for (size_t i = 0; i < num_threads; i++)
        tpool_apply(pool, NULL, NULL);
    for (size_t i = 0; i < num_threads; i++)
        pthread_join(pool->workers[i], NULL);
    free(pool->workers);
    jobqueue_destroy(pool->jobqueue);
    free(pool);
    return 0; // ?? what's this for?
}
