#include "job_queue.h"

#include <stddef.h>

int queue_init(job_queue_t *q)
{
    if (q == NULL) {
        return -1;
    }

    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutdown = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        return -1;
    }
    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        return -1;
    }
    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        return -1;
    }

    return 0;
}

void queue_destroy(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }

    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mutex);
}

int queue_push(job_queue_t *q, int job)
{
    if (q == NULL) {
        return -1;
    }
    if (pthread_mutex_lock(&q->mutex) != 0) {
        return -1;
    }

    while (q->count == QUEUE_CAPACITY && !q->shutdown) {
        int rc = pthread_cond_wait(&q->not_full, &q->mutex);
        if (rc != 0) {
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }
    }

    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    q->jobs[q->tail] = job;
    q->tail = (q->tail + 1) % QUEUE_CAPACITY;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

int queue_pop(job_queue_t *q, int *job)
{
    if (q == NULL || job == NULL) {
        return 0;
    }
    if (pthread_mutex_lock(&q->mutex) != 0) {
        return 0;
    }

    while (q->count == 0 && !q->shutdown) {
        int rc = pthread_cond_wait(&q->not_empty, &q->mutex);
        if (rc != 0) {
            pthread_mutex_unlock(&q->mutex);
            return 0;
        }
    }

    if (q->count == 0 && q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return 0;
    }

    *job = q->jobs[q->head];
    q->head = (q->head + 1) % QUEUE_CAPACITY;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return 1;
}

void queue_shutdown(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }
    if (pthread_mutex_lock(&q->mutex) != 0) {
        return;
    }
    q->shutdown = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}
