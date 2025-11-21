#ifndef DT_H
#define DT_H

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

// define bool if not defined
#ifndef bool
    typedef short bool;
#define true 1
#define false 0
#endif

#define TRUE true
#define FALSE false

// typedef Structure to represent a latch
typedef struct {
    pthread_rwlock_t lock;   // Read-write lock
} Latch;

// Function declarations and definitions

// Create and Destroy Latch Function
static inline void createLatch(Latch *latch) {
    pthread_rwlock_init(&latch->lock, NULL);
}

static inline void destroyLatch(Latch *latch) {
    pthread_rwlock_destroy(&latch->lock);
}

// Acquiring

// Acquire the latch for reading
static inline void lockLatchForRead(Latch *latch) {
    if (latch == NULL) {
        fprintf(stderr, "Error: Null latch pointer at location %p\n", (void *)latch);
        return;
    }
    printf("Locking latch for read operation at location %p\n", (void *)latch);
    int result = pthread_rwlock_rdlock(&latch->lock);
    if (result != 0) {
        fprintf(stderr, "Failed to acquire read lock: error code %d\n", result);
    }
}

// Acquire the latch for writing
static inline void lockLatchForWrite(Latch *latch) {
    if (latch == NULL) {
        fprintf(stderr, "Error: Null latch pointer at location %p\n", (void *)latch);
        return;
    }
    printf("Locking latch for write operation at location %p\n", (void *)latch);
    int result = pthread_rwlock_wrlock(&latch->lock);
    if (result != 0) {
        fprintf(stderr, "Failed to acquire write lock: error code %d\n", result);
    }
}

// Releasing

// Releasing latch after reading
static inline void releaseLatchAfterRead(Latch *latch) {
    printf("Releasing latch after reading at location %p\n", (void *)latch);
    int unlockResult = pthread_rwlock_unlock(&latch->lock);
    if (unlockResult != 0) {
        printf("Failed to release latch\n");
        printf("Error code: %d\n", unlockResult);
    }
}

// Releasing latch after writing
static inline void releaseLatchAfterWrite(Latch *latch) {
    printf("Releasing latch after writing at location %p\n", (void *)latch);
    int unlockResult = pthread_rwlock_unlock(&latch->lock);
    if (unlockResult != 0) {
        printf("Failed to release latch\n");
        printf("Error code: %d\n", unlockResult);
    }
}

#endif // DT_H
