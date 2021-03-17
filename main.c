#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#include "thread_pool.h"

#define PRECISION 100 /* upper bound in BPP sum */

/* Use Bailey–Borwein–Plouffe formula to approximate PI */
static void *bpp(void *arg)
{
    int k = *(int *)arg;
    double sum = (4.0 / (8 * k + 1)) - (2.0 / (8 * k + 4)) - (1.0 / (8 * k + 5)) -
                 (1.0 / (8 * k + 6));
    double *product = malloc(sizeof(double));
    if (product)
        *product = 1 / pow(16, k) * sum;
    return (void *)product;
}

int main()
{
    int bpp_args[PRECISION + 1];
    double bpp_sum = 0;
    tpool_t pool = tpool_create(4);
    tpool_future_t futures[PRECISION + 1];

    for (int i = 0; i <= PRECISION; i++) {
        bpp_args[i] = i;
        printf("Before %d\n", i);
        futures[i] = tpool_apply(pool, bpp, (void *)&bpp_args[i]);
        printf("After %d\n", i);
    }

    for (int i = 0; i <= PRECISION; i++) {
        printf("Before %d\n", i);
        double *result = tpool_future_get(futures[i], 0 /* blocking wait */);
        bpp_sum += *result;
        tpool_future_destroy(futures[i]);
        free(result);
        printf("After %d\n", i);
    }

    tpool_join(pool);
    printf("PI calculated with %d terms: %.15f\n", PRECISION + 1, bpp_sum);
    return 0;
}