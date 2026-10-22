#include "job_queue.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#define NUM_PRINTERS 4
#define DEFAULT_JOBS 50

typedef struct {
    int id;
    job_queue_t *queue;
    long jobs_completed;
} worker_arg_t;

typedef struct {
    int id;
    long jobs_to_submit;
    long first_job_id;
    job_queue_t *queue;
    int failed;
} producer_arg_t;

static void simulate_printing(void)
{
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000000L};
    (void)nanosleep(&ts, NULL);
}

static void *printer_worker(void *arg)
{
    worker_arg_t *w = (worker_arg_t *)arg;
    int job = 0;

    while (queue_pop(w->queue, &job)) {
        simulate_printing();
        w->jobs_completed++;
        printf("PROCESSED job=%d printer=%d\n", job, w->id);
        fflush(stdout);
    }
    return NULL;
}

static long parse_positive_long(const char *s, const char *name)
{
    char *end = NULL;
    errno = 0;
    long value = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || value <= 0) {
        fprintf(stderr, "Invalid %s: %s\n", name, s);
        exit(EXIT_FAILURE);
    }
    return value;
}

static int start_printers(job_queue_t *queue, pthread_t printers[], worker_arg_t workers[], int *created)
{
    *created = 0;
    for (int i = 0; i < NUM_PRINTERS; i++) {
        workers[i].id = i;
        workers[i].queue = queue;
        workers[i].jobs_completed = 0;
        int rc = pthread_create(&printers[i], NULL, printer_worker, &workers[i]);
        if (rc != 0) {
            fprintf(stderr, "pthread_create printer: %s\n", strerror(rc));
            return -1;
        }
        (*created)++;
    }
    return 0;
}

static int join_printers(pthread_t printers[], int created)
{
    int failed = 0;
    for (int i = 0; i < created; i++) {
        int rc = pthread_join(printers[i], NULL);
        if (rc != 0) {
            fprintf(stderr, "pthread_join printer: %s\n", strerror(rc));
            failed = 1;
        }
    }
    return failed ? -1 : 0;
}

static long completed_count(worker_arg_t workers[], int created)
{
    long total = 0;
    for (int i = 0; i < created; i++) {
        total += workers[i].jobs_completed;
    }
    return total;
}

static int run_core_server(long num_jobs)
{
    if (num_jobs > INT_MAX) {
        fprintf(stderr, "Job count must be <= %d\n", INT_MAX);
        return EXIT_FAILURE;
    }
    job_queue_t queue;
    pthread_t printers[NUM_PRINTERS];
    worker_arg_t workers[NUM_PRINTERS] = {0};
    int created = 0;

    if (queue_init(&queue) != 0) {
        fprintf(stderr, "queue_init failed\n");
        return EXIT_FAILURE;
    }
    if (start_printers(&queue, printers, workers, &created) != 0) {
        queue_shutdown(&queue);
        join_printers(printers, created);
        queue_destroy(&queue);
        return EXIT_FAILURE;
    }

    int submission_failed = 0;
    for (long job = 1; job <= num_jobs; job++) {
        if (queue_push(&queue, (int)job) != 0) {
            fprintf(stderr, "Failed to submit job %ld\n", job);
            submission_failed = 1;
            break;
        }
        printf("SUBMITTED job=%ld\n", job);
        fflush(stdout);
    }

    queue_shutdown(&queue);
    int join_failed = join_printers(printers, created) != 0;
    long total = completed_count(workers, created);
    printf("TOTAL_COMPLETED %ld\n", total);
    queue_destroy(&queue);
    return (!submission_failed && !join_failed && total == num_jobs) ? EXIT_SUCCESS : EXIT_FAILURE;
}

static void *producer_worker(void *arg)
{
    producer_arg_t *p = (producer_arg_t *)arg;
    for (long i = 0; i < p->jobs_to_submit; i++) {
        long id = p->first_job_id + i;
        if (queue_push(p->queue, (int)id) != 0) {
            fprintf(stderr, "Producer %d failed to submit job %ld\n", p->id, id);
            p->failed = 1;
            break;
        }
        printf("SUBMITTED producer=%d job=%ld\n", p->id, id);
        fflush(stdout);
    }
    return NULL;
}

static int run_bonus_server(long producers, long jobs_per_producer)
{
    if (producers > INT_MAX || jobs_per_producer > INT_MAX ||
        producers > INT_MAX / jobs_per_producer) {
        fprintf(stderr, "Total job count must be <= %d and producer count must be <= %d\n", INT_MAX, INT_MAX);
        return EXIT_FAILURE;
    }
    long total_jobs = producers * jobs_per_producer;
    if (producers > (long)(SIZE_MAX / sizeof(pthread_t)) ||
        producers > (long)(SIZE_MAX / sizeof(producer_arg_t))) {
        fprintf(stderr, "Too many producers\n");
        return EXIT_FAILURE;
    }

    job_queue_t queue;
    pthread_t printers[NUM_PRINTERS];
    worker_arg_t workers[NUM_PRINTERS] = {0};
    pthread_t *producer_threads = calloc((size_t)producers, sizeof(*producer_threads));
    producer_arg_t *producer_args = calloc((size_t)producers, sizeof(*producer_args));
    if (producer_threads == NULL || producer_args == NULL) {
        fprintf(stderr, "Failed to allocate producer data\n");
        free(producer_threads);
        free(producer_args);
        return EXIT_FAILURE;
    }
    if (queue_init(&queue) != 0) {
        fprintf(stderr, "queue_init failed\n");
        free(producer_threads);
        free(producer_args);
        return EXIT_FAILURE;
    }

    int printer_count = 0;
    if (start_printers(&queue, printers, workers, &printer_count) != 0) {
        queue_shutdown(&queue);
        join_printers(printers, printer_count);
        queue_destroy(&queue);
        free(producer_threads);
        free(producer_args);
        return EXIT_FAILURE;
    }

    long producer_count = 0;
    int producer_create_failed = 0;
    for (long i = 0; i < producers; i++) {
        producer_args[i].id = (int)i;
        producer_args[i].jobs_to_submit = jobs_per_producer;
        producer_args[i].first_job_id = i * jobs_per_producer + 1;
        producer_args[i].queue = &queue;
        producer_args[i].failed = 0;
        int rc = pthread_create(&producer_threads[i], NULL, producer_worker, &producer_args[i]);
        if (rc != 0) {
            fprintf(stderr, "pthread_create producer: %s\n", strerror(rc));
            producer_create_failed = 1;
            break;
        }
        producer_count++;
    }

    /* All created producers must finish before the queue is shut down. */
    int producer_join_failed = 0;
    for (long i = 0; i < producer_count; i++) {
        int rc = pthread_join(producer_threads[i], NULL);
        if (rc != 0) {
            fprintf(stderr, "pthread_join producer: %s\n", strerror(rc));
            producer_join_failed = 1;
        }
    }
    queue_shutdown(&queue);
    int printer_join_failed = join_printers(printers, printer_count) != 0;
    long completed = completed_count(workers, printer_count);
    printf("TOTAL_COMPLETED %ld\n", completed);

    int producer_failed = 0;
    for (long i = 0; i < producer_count; i++) {
        if (producer_args[i].failed) producer_failed = 1;
    }
    queue_destroy(&queue);
    free(producer_threads);
    free(producer_args);
    return (!producer_create_failed && !producer_join_failed && !producer_failed &&
            !printer_join_failed && producer_count == producers && completed == total_jobs)
               ? EXIT_SUCCESS : EXIT_FAILURE;
}

int main(int argc, char *argv[])
{
    if (argc >= 2 && strcmp(argv[1], "--bonus") == 0) {
        if (argc != 4) {
            fprintf(stderr, "Usage: %s --bonus <producers> <jobs-per-producer>\n", argv[0]);
            return EXIT_FAILURE;
        }
        long producers = parse_positive_long(argv[2], "producer count");
        long jobs = parse_positive_long(argv[3], "jobs per producer");
        return run_bonus_server(producers, jobs);
    }

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [number-of-jobs]\n", argv[0]);
        return EXIT_FAILURE;
    }

    long num_jobs = (argc == 2) ? parse_positive_long(argv[1], "job count") : DEFAULT_JOBS;
    return run_core_server(num_jobs);
}
