#include "buffer_mgr.h"
#include "stdlib.h"
#include <unistd.h>

int writtenToDisk = 0;
int readFromDisk = 0;
int lruCounter = 0;
bool isInitialized_bp = false;

int active_threads = 0;
bool buffer_pool_shutting_down = false;

// Global mutex lock for buffer pool initialization
pthread_mutex_t buffer_pool_init_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t bp_unique_init_mutex = PTHREAD_MUTEX_INITIALIZER;
// Global condition variable
pthread_cond_t buffer_pool_cond = PTHREAD_COND_INITIALIZER;

/*
 * Initializes a buffer pool with the specified parameters.
 *
 * Parameters:
 * - bm: Pointer to the buffer pool structure to be initialized.
 * - pageFileName: Name of the page file associated with the buffer pool.
 * - numPages: Number of page frames in the buffer pool.
 * - strategy: Replacement strategy to be used by the buffer pool.
 * - stratData: Additional parameters for the replacement strategy.
 *
 * Returns:
 * - RC_OK if the buffer pool is successfully initialized, otherwise an error code.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
    // Acquire the global mutex lock before initializing the buffer pool
    pthread_mutex_lock(&bp_unique_init_mutex);

    // Check if the buffer pool has already been initialized
    if (isInitialized_bp) {
        printf("Buffer pool already initialized.\n");
        pthread_mutex_unlock(&bp_unique_init_mutex);
        return RC_OK;
    }

    printf("Initializing the Buffer Pool.\n");

    // Check if the file exists
    if (access(pageFileName, F_OK) != -1) {
        printf("File '%s' exists.\n", pageFileName);
    } else {
        pthread_mutex_unlock(&bp_unique_init_mutex);
        return RC_FILE_NOT_FOUND; // Define appropriate error code
    }

    // Allocate memory for the buffer pool
    bm->mgmtData = malloc(sizeof(Frames) * numPages);
    if (bm->mgmtData == NULL) {
        free(bm->mgmtData);
        pthread_mutex_unlock(&bp_unique_init_mutex);
        return RC_BP_INIT_ERROR;
    }

    // Initialize the individual frames in the buffer pool
    Frames *frames = (Frames *)bm->mgmtData;

    // Allocate memory for page latches
    frames->pageLatches = malloc(numPages * sizeof(Latch));
    if (frames->pageLatches == NULL) {
        free(bm->mgmtData);
        pthread_mutex_unlock(&bp_unique_init_mutex);
        return RC_BP_INIT_ERROR;
    }

    for (int i = 0; i < numPages; i++) {
        // Other initialization
        frames[i].memPage = (SM_PageHandle) malloc(PAGE_SIZE);
        if (frames[i].memPage == NULL) {
            for (int j = 0; j < i; j++) {
                free(frames[j].memPage);
            }
            free(frames->pageLatches);
            free(bm->mgmtData);
            // Handle memory allocation error
            pthread_mutex_unlock(&bp_unique_init_mutex);
            return RC_BP_INIT_ERROR;
        }

        frames[i].pageNumber = NO_PAGE;
        frames[i].dirty = false;
        frames[i].fix_cnt = 0;
        frames[i].lruOrder = 0;
        createLatch(&(frames->pageLatches[i]));
    }

    int *data = (int *)stratData;
    if (data != NULL) {
        // Use the value of the strategy-specific data
        bm->stratParam = *data;
        // Proceed with initializing the buffer pool using the value
    }

    // Initialize other properties of the buffer pool
    bm->pageFile = (char*)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;

    writtenToDisk = 0;
    readFromDisk = 0;

    printf("Buffer Pool has initialized.\n");
    isInitialized_bp=true;

    // Release the global mutex lock after initialization
    pthread_mutex_unlock(&bp_unique_init_mutex);

    return RC_OK;
}

/*
 * Destroys a buffer pool, freeing up all associated resources.
 * If the buffer pool contains any dirty pages with a fix count of 0,
 * they are written back to disk before destroying the pool.
 *
 * Parameters:
 * - bm: Pointer to the buffer pool structure to be shut down.
 *
 * Returns:
 * - RC_OK if the buffer pool is successfully shut down, otherwise an error code.
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
    printf("Shutting down the Buffer Pool.\n");
    if (isInitialized_bp == false) {
        pthread_mutex_destroy(&buffer_pool_init_mutex);
        return RC_BP_SHUNTDOWN_ERROR;
    }

    // Acquire the global mutex lock
    pthread_mutex_lock(&buffer_pool_init_mutex);

    Frames *frames = (Frames *) bm->mgmtData;

    // Set a flag to indicate that the buffer pool is shutting down
    buffer_pool_shutting_down = true;

    // Wait for all threads to complete their operations
    while (active_threads > 0) {
        pthread_cond_wait(&buffer_pool_cond, &buffer_pool_init_mutex);
    }

    // Write dirty page back to disk
    forceFlushPool(bm);

    // Now free the memory for each page frame
    for (int i = 0; i < bm->numPages; i++) {
        if (frames[i].memPage != NULL) {
            free(frames[i].memPage);
            frames[i].memPage = NULL; // Avoid dangling pointer
        }
        if (&(frames->pageLatches[i]) != NULL) {
            destroyLatch(&(frames->pageLatches[i]));
        }
    }

    // Free memory for the array of page latches
    free(frames->pageLatches);
    frames->pageLatches = NULL;

    // Free memory associated with the buffer pool
    free(frames);
    bm->mgmtData = NULL;

    isInitialized_bp = false;

    // Release the global mutex lock
    pthread_mutex_unlock(&buffer_pool_init_mutex);

    // Destroy the mutex lock and condition variable
    pthread_mutex_destroy(&buffer_pool_init_mutex);
    pthread_cond_destroy(&buffer_pool_cond);

    printf("Buffer Pool has shut down.\n");
    return RC_OK;
}

/*
 * Writes all dirty pages with a fix count of 0 from the buffer pool to disk.
 *
 * Parameters:
 * - bm: Pointer to the buffer pool structure.
 *
 * Returns:
 * - RC_OK if all dirty pages are successfully written to disk, otherwise an error code.
 */
RC forceFlushPool(BM_BufferPool *const bm) {
    printf("Forcing flush the Buffer Pool.\n");
    if (isInitialized_bp == false) {
        return RC_BP_FLUSHPOOL_FAILED;
    }

    Frames *frames = (Frames *) bm->mgmtData;
    int numPages = bm->numPages;
    int check_error = 0;

    // Check for pinned pages
    for (int i = 0; i< numPages; i++) {
        if (frames[i].dirty == true && frames[i].fix_cnt == 0) {
            // Acquire a latch to ensure exclusive access to the memory page
            lockLatchForWrite(&(frames->pageLatches[i]));

            SM_FileHandle fHandle;
            openPageFile(bm->pageFile, &fHandle);
            writeBlock(frames[i].pageNumber, &fHandle, frames[i].memPage);
            closePageFile(&fHandle);
            frames[i].dirty = false;
            writtenToDisk++;
            releaseLatchAfterWrite(&(frames->pageLatches[i]));
        } else {
            check_error++;
        }
        if (frames[i].fix_cnt != 0) {
            return RC_BP_FLUSHPOOL_FAILED;
        }
    }

    if (check_error == numPages) {
        return RC_BP_FLUSHPOOL_FAILED;
    } else {
        printf("Finished force flush pool.\n");
        return RC_OK;
    }
}

/*
 * FIFO (First-In-First-Out) page replacement strategy.
 * This function implements the FIFO page replacement algorithm,
 * which selects the page that was brought into memory first for eviction.
 *
 * @param bm     Buffer pool containing information about the buffer pool
 * @param page   Pointer to the page to be replaced
 * @return       RC_OK on success, or an error code otherwise
 */
RC FIFO (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    printf("Using FIFO strategy.\n");
    Frames *frames = (Frames *) bm->mgmtData;
    int FIFO_PageIndex;
    int check_error = 0;

    FIFO_PageIndex = readFromDisk % bm->numPages;

    for (int i = 0; i< bm->numPages; i++) {
        // Handle using pages
        if (frames[FIFO_PageIndex].fix_cnt == 0) {
            if (frames[FIFO_PageIndex].dirty) {
                lockLatchForWrite(&(frames->pageLatches[FIFO_PageIndex]));
                SM_FileHandle fHandle;
                openPageFile(bm->pageFile, &fHandle);
                writeBlock(frames[FIFO_PageIndex].pageNumber, &fHandle, frames[FIFO_PageIndex].memPage);
                closePageFile(&fHandle);
                frames[FIFO_PageIndex].dirty = false;
                writtenToDisk++;
                releaseLatchAfterWrite(&(frames->pageLatches[FIFO_PageIndex]));
            }

            // Read page from disk into a new frame
            lockLatchForRead(&(frames->pageLatches[FIFO_PageIndex]));
            SM_FileHandle fHandle;
            openPageFile(bm->pageFile, &fHandle);
            ensureCapacity(pageNum, &fHandle);
            readBlock(pageNum, &fHandle, frames[FIFO_PageIndex].memPage);
            closePageFile(&fHandle);
            releaseLatchAfterRead(&(frames->pageLatches[FIFO_PageIndex]));

            readFromDisk++;

            // Update frame information with the new page
            lruCounter++;
            frames[FIFO_PageIndex].pageNumber = pageNum;
            frames[FIFO_PageIndex].dirty = false;
            frames[FIFO_PageIndex].fix_cnt = 1;
            frames[FIFO_PageIndex].lruOrder = lruCounter;
            page->pageNum = pageNum;
            page->data = frames[FIFO_PageIndex].memPage;
            break;
        } else {
            FIFO_PageIndex++;
            FIFO_PageIndex = FIFO_PageIndex % bm->numPages;
            check_error++;
        }
    }
    // If all pages are pinned, return an error
    if (check_error == bm->numPages) {
        return RC_BP_PIN_ERROR;
    }
    return RC_OK;
}

/*
 * LRU (Least Recently Used) page replacement strategy.
 * This function implements the LRU page replacement algorithm,
 * which selects the page that has not been used for the longest time for eviction.
 *
 * @param bm     Buffer pool containing information about the buffer pool
 * @param page   Pointer to the page to be replaced
 * @return       RC_OK on success, or an error code otherwise
 */
RC LRU (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    printf("Using LRU strategy.\n");
    Frames *frames = (Frames *) bm->mgmtData;
    int LRU_PageIndex = 0;
    int comNum = frames[0].lruOrder;

    // Find the index of the least recently used page
    for (int i = 1; i < bm->numPages; i++) {
        if (frames[i].lruOrder < comNum) {
            comNum = frames[i].lruOrder;
            LRU_PageIndex = i;
        }
    }

    // Check if the least recently used page is dirty and write it back to disk
    if (frames[LRU_PageIndex].dirty) {
        lockLatchForWrite(&(frames->pageLatches[LRU_PageIndex]));
        SM_FileHandle fHandle;
        openPageFile(bm->pageFile, &fHandle);
        writeBlock(frames[LRU_PageIndex].pageNumber, &fHandle, frames[LRU_PageIndex].memPage);
        closePageFile(&fHandle);
        frames[LRU_PageIndex].dirty = false;
        writtenToDisk++;
        releaseLatchAfterWrite(&(frames->pageLatches[LRU_PageIndex]));
    }

    // Read the new page from disk into the selected frame
    lockLatchForRead(&(frames->pageLatches[LRU_PageIndex]));
    SM_FileHandle fHandle;
    openPageFile(bm->pageFile, &fHandle);
    ensureCapacity(pageNum, &fHandle);
    readBlock(pageNum, &fHandle, frames[LRU_PageIndex].memPage);
    closePageFile(&fHandle);
    releaseLatchAfterRead(&(frames->pageLatches[LRU_PageIndex]));

    readFromDisk++;

    // Update frame information with the new page and its usage order
    lruCounter++;
    frames[LRU_PageIndex].pageNumber = pageNum;
    frames[LRU_PageIndex].dirty = false;
    frames[LRU_PageIndex].fix_cnt = 1;
    frames[LRU_PageIndex].lruOrder = lruCounter;
    page->pageNum = pageNum;
    page->data = frames[LRU_PageIndex].memPage;

    return RC_OK;
}

// Function to swap two elements
void swap(int* a, int* b) {
    int temp = *a;
    *a = *b;
    *b = temp;
}

// Function to partition the array and return the pivot index
int partition(int arr[], int low, int high) {
    int pivot = arr[high]; // Choose the rightmost element as the pivot
    int i = low - 1; // Index of smaller element

    for (int j = low; j < high; j++) {
        // If current element is smaller than or equal to pivot
        if (arr[j] <= pivot) {
            i++; // Increment index of smaller element
            swap(&arr[i], &arr[j]);
        }
    }
    swap(&arr[i + 1], &arr[high]);
    return i + 1;
}

// Quicksort function in ascending order
void quickSort(int arr[], int low, int high) {
    if (low < high) {
        // Partition the array
        int pi = partition(arr, low, high);

        // Recursively sort elements before partition and after partition
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

/*
 * LRU-K (Least Recently Used with K-Value) page replacement strategy.
 * This function implements the LRU-K page replacement algorithm,
 * which selects the page that has not been used for the longest time within the last K references for eviction.
 *
 * @param bm     Buffer pool containing information about the buffer pool
 * @param page   Pointer to the page to be replaced
 * @return       RC_OK on success, or an error code otherwise
 */
RC LRU_K (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    printf("Using LRU strategy.\n");
    Frames *frames = (Frames *) bm->mgmtData;
    int k = bm->stratParam;
    int orderNum[bm->numPages];
    int LRU_PageIndex = 0;

    // Populate the array with the order numbers of pages
    for (int i = 0; i < bm->numPages; i++) {
        orderNum[i] = frames[i].lruOrder;
    }

    // Sort the order numbers in ascending order using quicksort
    quickSort(orderNum, 0, bm->numPages - 1);

    // Find the page with the K-th least recent order number
    for (int i = 0; i < bm->numPages; i++) {
        if(orderNum[bm->numPages - k] == frames[i].lruOrder) {
            LRU_PageIndex = i;
            break;
        }
    }

    // Check if the selected page is dirty and write it back to disk
    if (frames[LRU_PageIndex].dirty) {
        lockLatchForWrite(&(frames->pageLatches[LRU_PageIndex]));
        SM_FileHandle fHandle;
        openPageFile(bm->pageFile, &fHandle);
        writeBlock(frames[LRU_PageIndex].pageNumber, &fHandle, frames[LRU_PageIndex].memPage);
        closePageFile(&fHandle);
        frames[LRU_PageIndex].dirty = false;
        writtenToDisk++;
        releaseLatchAfterWrite(&(frames->pageLatches[LRU_PageIndex]));
    }

    // Read the new page from disk into the selected frame
    lockLatchForRead(&(frames->pageLatches[LRU_PageIndex]));
    SM_FileHandle fHandle;
    openPageFile(bm->pageFile, &fHandle);
    ensureCapacity(pageNum, &fHandle);
    readBlock(pageNum, &fHandle, frames[LRU_PageIndex].memPage);
    closePageFile(&fHandle);
    releaseLatchAfterRead(&(frames->pageLatches[LRU_PageIndex]));

    readFromDisk++;

    // Update frame information with the new page and its usage order
    lruCounter++;
    frames[LRU_PageIndex].pageNumber = pageNum;
    frames[LRU_PageIndex].dirty = false;
    frames[LRU_PageIndex].fix_cnt = 1;
    frames[LRU_PageIndex].lruOrder = lruCounter;
    page->pageNum = pageNum;
    page->data = frames[LRU_PageIndex].memPage;

    return RC_OK;
}

/*
 * Marks a page in the buffer pool as dirty, indicating that it has been modified.
 *
 * @param bm   Buffer pool containing information about the buffer pool
 * @param page Pointer to the page to be marked as dirty
 * @return     RC_OK on success, or an error code otherwise
 */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    printf("Marking dirty page.\n");
    Frames *frames = (Frames *) bm->mgmtData;
    int check_error = 0;

    // Iterate through the frames to find the specified page
    for (int i = 0; i< bm->numPages; i++) {
        if (frames[i].pageNumber == page->pageNum) {
            frames[i].dirty = true;
        } else {
            check_error++;
        }
    }

    // If the specified page is not found in any frame, return error
    if(check_error == bm->numPages) {
        return RC_BP_UNMARK_ERROR;
    } else {
        printf("Marked dirty page.\n");
        return RC_OK;
    }
}

/*
 * Unpins a page in the buffer pool, indicating that it is no longer in use by the calling process.
 *
 * @param bm   Buffer pool containing information about the buffer pool
 * @param page Pointer to the page to be unpinned
 * @return     RC_OK on success, or an error code otherwise
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    printf("Unpinning page.\n");
    Frames *frames = (Frames *) bm->mgmtData;

    // Iterate through the frames to find the specified page
    for (int i = 0; i< bm->numPages; i++) {
        if (frames[i].pageNumber == page->pageNum) {
            if (frames[i].fix_cnt > 0) {
                frames[i].fix_cnt--;
                printf("Unpinned page.\n");
                return RC_OK;
            } else {
                printf("Page is already unpinned.\n");
                return RC_BP_UNPIN_ERROR;
            }
        }
    }

    // If the specified page is not found in any frame, return error
    printf("Page not found in buffer pool.\n");
    return RC_BP_UNPIN_ERROR;
}

/*
 * Forces a dirty page to disk, ensuring that any modifications are written back to the page file.
 *
 * @param bm   Buffer pool containing information about the buffer pool
 * @param page Pointer to the page to be forced to disk
 * @return     RC_OK on success, or an error code otherwise
 */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    printf("Forcing dirty page to disk.\n");
    Frames *frames = (Frames *) bm->mgmtData;
    int numPages = bm->numPages;
    int check_error = 0;

    // Iterate through the frames to find the specified page
    for (int i = 0; i< numPages; i++) {
        if (frames[i].pageNumber == page->pageNum) {
            lockLatchForWrite(&(frames->pageLatches[i]));
            SM_FileHandle fHandle;
            openPageFile(bm->pageFile, &fHandle);
            writeBlock(frames[i].pageNumber, &fHandle, frames[i].memPage);
            closePageFile(&fHandle);
            frames[i].dirty = false;
            writtenToDisk++;
            releaseLatchAfterWrite(&(frames->pageLatches[i]));
        } else {
            check_error++;
        }
    }

    // If the specified page is not found in any frame, return error
    if(check_error == bm->numPages) {
        return RC_BP_FORCE_ERROR;
    } else {
        return RC_OK;
    }
}

/*
 * Pins a page in the buffer pool, ensuring that it is available for use by the client.
 *
 * @param bm      Buffer pool containing information about the buffer pool
 * @param page    Pointer to the page handle structure to store information about the pinned page
 * @param pageNum Page number to be pinned
 * @return        RC_OK on success, or an error code otherwise
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum) {

    printf("Pinning page.\n");
    if (!isInitialized_bp) {
        return RC_BP_PIN_ERROR;
    }

    Frames *frames = (Frames *) bm->mgmtData;

    if (pageNum < 0) {
        return RC_BP_PIN_ERROR;
    }

    // Check if page is already in buffer pool
    for (int i = 0; i< bm->numPages; i++) {
        if (frames[i].pageNumber == pageNum) {
            lruCounter++;
            frames[i].fix_cnt++;
            frames[i].lruOrder = lruCounter;
            page->pageNum = pageNum;
            page->data = frames[i].memPage;
            return RC_OK;
        }
    }

    // Page is not in buffer pool, find a free slot
    int freeSlotIndex = -1;
    for (int i = 0; i < bm->numPages; i++) {
        if (frames[i].pageNumber == NO_PAGE) {
            freeSlotIndex = i;
            break;
        }
    }

    // Free slot found
    if (freeSlotIndex != -1) {
        lockLatchForRead(&(frames->pageLatches[freeSlotIndex]));
        // Read page from disk into the selected frame
        SM_FileHandle fHandle;
        openPageFile(bm->pageFile, &fHandle);
        ensureCapacity(pageNum, &fHandle);
        readBlock(pageNum, &fHandle, frames[freeSlotIndex].memPage);
        closePageFile(&fHandle);
        releaseLatchAfterRead(&(frames->pageLatches[freeSlotIndex]));

        readFromDisk++;

        // Update frame details
        frames[freeSlotIndex].fix_cnt = 1;
        frames[freeSlotIndex].pageNumber = pageNum;
        page->pageNum = pageNum;
        page->data = frames[freeSlotIndex].memPage;

        return RC_OK;
    }

    // No free slot found, call the appropriate replacement strategy function
    switch (bm->strategy) {
        case RS_FIFO:
            return FIFO(bm, page, pageNum);
        case RS_LRU:
            return LRU(bm, page, pageNum);
        case RS_LRU_K:
            return LRU_K(bm, page, pageNum);
        default:
            return RC_BP_PIN_ERROR;
    }
}


// Statistics Interface
/*
 * Retrieves the page numbers stored in each frame of the buffer pool.
 *
 * @param bm Buffer pool containing information about the buffer pool
 * @return   An array containing the page numbers stored in each frame,
 *           or NULL if memory allocation fails
 */
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    Frames *frames = (Frames *)bm->mgmtData;
    int numPages = bm->numPages;
    PageNumber *contents = malloc(sizeof(PageNumber) * numPages);

    if (contents == NULL) {
        // Handle memory allocation failure
        return NULL;
    }

    // Iterate over all page frames
    for (int i = 0; i < numPages; i++) {
        // If the frame is empty, assign NO_PAGE
        if (frames[i].pageNumber == NO_PAGE) {
            contents[i] = NO_PAGE;
        } else {
            // Otherwise, assign the page number stored in the frame
            contents[i] = frames[i].pageNumber;
        }
    }

    return contents;
}

/*
 * Retrieves the dirty flags of each page stored in the buffer pool.
 *
 * @param bm Buffer pool containing information about the buffer pool
 * @return   An array containing the dirty flags of each page,
 *           where true indicates the page is dirty (modified),
 *           false indicates the page is clean (not modified),
 *           or NULL if memory allocation fails
 */
bool *getDirtyFlags (BM_BufferPool *const bm) {
    Frames *frames = (Frames *)bm->mgmtData;
    int numPages = bm->numPages;
    bool *dirtyFlags = malloc(sizeof(bool) * numPages);

    // Iterate over all page frames
    for (int i = 0; i < numPages; i++) {
        // If the frame is empty, it's considered clean
        if (frames[i].pageNumber == NO_PAGE) {
            dirtyFlags[i] = false;
        } else {
            // Otherwise, get the dirty flag of the page
            dirtyFlags[i] = frames[i].dirty;
        }
    }
    return dirtyFlags;
}

/*
 * Retrieves the fix counts of each page stored in the buffer pool.
 *
 * @param bm Buffer pool containing information about the buffer pool
 * @return   An array containing the fix counts of each page,
 *           indicating the number of clients currently using the page,
 *           or NULL if memory allocation fails
 */
int *getFixCounts (BM_BufferPool *const bm) {
    Frames *frames = (Frames *)bm->mgmtData;
    int numPages = bm->numPages;
    int *fixCounts = malloc(sizeof(bool) * numPages);

    // Iterate over all page frames
    for (int i = 0; i < numPages; i++) {
        // If the frame is empty, it's considered clean
        if (frames[i].pageNumber == NO_PAGE) {
            fixCounts[i] = false;
        } else {
            // Otherwise, get the dirty flag of the page
            fixCounts[i] = frames[i].fix_cnt;
        }
    }
    return fixCounts;
}

/*
 * Retrieves the total number of read operations performed on the buffer pool.
 *
 * @param bm Buffer pool containing information about the buffer pool
 * @return   The total number of read operations performed on the buffer pool
 */
int getNumReadIO (BM_BufferPool *const bm) {
    return (readFromDisk + 1);
}

/*
 * Retrieves the total number of write operations performed on the buffer pool.
 *
 * @param bm Buffer pool containing information about the buffer pool
 * @return   The total number of write operations performed on the buffer pool
 */
int getNumWriteIO (BM_BufferPool *const bm) {
    return writtenToDisk;
}