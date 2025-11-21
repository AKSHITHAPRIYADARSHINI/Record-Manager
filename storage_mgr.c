#include "storage_mgr.h"
#include "dberror.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

// Default setting of the storage manager status
bool isInitialized=false;

/* manipulating page files */
void initStorageManager () {
    isInitialized = true;
    printf("Initializing the Storage Manager.\n");
}

RC createPageFile (char *fileName) {
    printf("Page file starts creating.\n");
    FILE *fileExists = fopen(fileName,"r");
    if (fileExists != NULL) {
        fclose(fileExists);
        fprintf(stderr, "Error: File already exists.\n");
        exit(1);
    }

    FILE *file = fopen(fileName, "w+");
    if (file == NULL) {
        exit(1);
    }

    // Buffer to store '\0' bytes
    char newBuffer[PAGE_SIZE];
    memset(newBuffer, 0, PAGE_SIZE);

    // Write to the file
    fwrite(newBuffer, sizeof(char), PAGE_SIZE, file);

    // Close file
    fclose(file);

    printf("Page file is created.\n");
    return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
    printf("Page file opening.\n");
    // Check for existence
    FILE *fileExists = fopen(fileName,"r");
    if (fileExists == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    fclose(fileExists);

    fHandle->fileName = fileName;
    if (fHandle->fileName == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    fHandle->totalNumPages = 0;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = NULL;

    // Open the file
    fHandle->mgmtInfo = fopen(fileName, "r+");

    // Get the total number of pages
    if (fseek(fHandle->mgmtInfo, 0, SEEK_END) != 0) {
        fprintf(stderr, "Error: Unable to seek to the end of the file.\n");
        fclose(fHandle->mgmtInfo);
        return RC_READ_FAILED;
    }

    long fileSize = ftell(fHandle->mgmtInfo);

    if (fileSize == -1) {
        fprintf(stderr, "Error: Unable to determine the file size.\n");
        fclose(fHandle->mgmtInfo);
        return RC_READ_FAILED;
    }

    fHandle->totalNumPages = ftell(fHandle->mgmtInfo) / PAGE_SIZE;

    // Rewind the file
    rewind(fHandle->mgmtInfo);

    return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle) {
    printf("Page file closing.\n");

    // Check if file handle is initialized
    if (fHandle->mgmtInfo == NULL) {
        return  RC_FILE_NOT_FOUND;
    }

    if (fclose(fHandle->mgmtInfo) == 0) {
        fHandle->mgmtInfo = NULL;
        return RC_OK;
    } else {
        return  RC_FILE_NOT_FOUND;
    }
}

RC destroyPageFile (char *fileName) {
    printf("Page file deleting.\n");
    // Check for existence
    FILE *fileExists = fopen(fileName,"r");
    if (fileExists == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    fclose(fileExists);

    // Remove file
    if(remove(fileName) == 0) {
        return RC_OK;
    } else {
        return RC_DELETE_FAILED;
    }
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    printf("Reading Blocks.\n");
    // Check the validation
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Move the file pointer to the pageNum
    if (fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
        fprintf(stderr, "Error: Unable to move the file pointer to the pageNum.\n");
        return RC_READ_FAILED;
    }

    // Stores its content in the memory pointed to by the memPage page handle
    fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

    // Update current position
    fHandle->curPagePos = pageNum;

    return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle) {
    // Check for NULL pointer
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (!isInitialized) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    return readBlock(0, fHandle, memPage);

}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Check the page 0 condition
    if (fHandle->curPagePos == 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    return readBlock(fHandle->curPagePos-1, fHandle, memPage);
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int curPos = getBlockPos(fHandle);

    return readBlock(curPos, fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Check the end page condition
    if (fHandle->curPagePos >= fHandle->totalNumPages-1) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    return readBlock(fHandle->curPagePos+1, fHandle, memPage);
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    fHandle->curPagePos = fHandle->totalNumPages-1;

    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_WRITE_FAILED;
    }

    // Move the pointer to the target page
    if (fseek(fHandle->mgmtInfo, pageNum *PAGE_SIZE, SEEK_SET) != 0) {
        fprintf(stderr, "Error: Unable to move the file pointer to the pageNum.\n");
        return RC_READ_FAILED;
    }

    // Write Content to the file
    fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

    fHandle->curPagePos = pageNum;

    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int curPos = getBlockPos(fHandle);

    return writeBlock(curPos, fHandle, memPage);
}

RC appendEmptyBlock (SM_FileHandle *fHandle) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Move the pointer to the end
    if (fseek(fHandle->mgmtInfo, 0, SEEK_END) != 0) {
        fprintf(stderr, "Error: Unable to move the file pointer to the pageNum.\n");
        return RC_READ_FAILED;
    };

    // Get zero bytes buffer
    SM_PageHandle zeroPg = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));

    // Write the page to the file
    size_t appendContent = fwrite(zeroPg, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

    // Check Errors
    if (appendContent != PAGE_SIZE) {
        free(zeroPg);
        return RC_WRITE_FAILED;
    }

    // Update total number and position
    fHandle->totalNumPages++;
    fHandle->curPagePos = fHandle->totalNumPages - 1;

    free(zeroPg);

    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    // Check validation
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (fHandle->totalNumPages < numberOfPages) {
        int increasePg = numberOfPages - fHandle->totalNumPages;
        for (int i = 0; i < increasePg; i++) {
            RC result = appendEmptyBlock(fHandle);
            if (result != RC_OK) {
                return result;
            }
        }
    }
    return RC_OK;
}