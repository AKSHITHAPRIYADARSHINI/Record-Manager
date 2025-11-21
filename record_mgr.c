#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "stdlib.h"
#include <string.h>
#include "record_mgr.h"
#include <stdio.h>
#include <stdbool.h>

/* 
 * Global configuration values
 * These can be adjusted based on system requirements
 */
#define BUFFER_PAGE_LIMIT 5
#define INVALID_PAGE_NUM -1
#define INVALID_SLOT_NUM -1
#define DELETED_RECORD_MARKER 0xFD

/* 
 * Forward declarations of helper functions
 * These are internal functions not exposed in the header
 */
static int computeRecordSize(Schema *schema);
static int findFreePageIndex(RM_TableData *table);
static int locateFreeSlot(char *pageData, int recordCount);
static bool isValidRecordID(RID id, int totalPages);
static void initPageDirectoryEntry(PageDirectoryEntry *entry, int pageId);
static RC savePageDirectoryToDisk(RM_TableData *table);
static RC loadPageDirectoryFromDisk(RM_TableData *table);
static void updatePageStatistics(RM_TableData *table, int pageIdx, int spaceChange, bool recordAdded);
static int calculateAttributeOffset(Schema *schema, int attrIdx);
static int ceilDivision(int numerator, int denominator);

/*
 * Record Manager Lifecycle Functions
 */

/* 
 * Bootstraps the record manager system
 * Must be called before any other record manager functions
 */
RC initRecordManager(void *customConfig) {
    printf("Starting record manager initialization process...\n");
    
    // Initialize the underlying storage manager
    initStorageManager();
    
    // We could use customConfig for advanced settings in future versions
    if (customConfig != NULL) {
        printf("Custom configuration provided but not used in current implementation\n");
    }
    
    printf("Record manager successfully initialized\n");
    return RC_OK;
}

/* 
 * Gracefully shuts down the record manager system
 * Should be called when record manager is no longer needed
 */
RC shutdownRecordManager() {
    printf("Executing record manager shutdown sequence\n");
    
    // In this implementation, we don't maintain global state
    // Future versions might need to clean up shared resources here
    
    printf("Record manager shutdown completed successfully\n");
    return RC_OK;
}

/*
 * Table Operations
 */

/* 
 * Creates a new table with the given name and schema
 * This involves creating a page file and storing schema information
 */
RC createTable(char *tableName, Schema *schema) {
    printf("Creating new table '%s'...\n", tableName);
    
    // Validate input parameters
    if (!tableName || !schema) {
        printf("Error: Table name or schema is NULL\n");
        return RC_INVALID_INPUT;
    }
    
    // Step 1: Create the underlying page file
    RC status = createPageFile(tableName);
    if (status != RC_OK) {
        printf("Error: Failed to create page file for table '%s'\n", tableName);
        return status;
    }
    
    // Step 2: Open the newly created file
    SM_FileHandle fileHandle;
    status = openPageFile(tableName, &fileHandle);
    if (status != RC_OK) {
        printf("Error: Failed to open page file for table '%s'\n", tableName);
        return status;
    }
    
    // Step 3: Prepare the schema page (page 0)
    char *schemaPage = calloc(1, PAGE_SIZE);
    if (!schemaPage) {
        closePageFile(&fileHandle);
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Step 4: Serialize schema information to the page
    int position = 0;
    
    // Write number of attributes
    if (position + sizeof(int) > PAGE_SIZE) {
        free(schemaPage);
        closePageFile(&fileHandle);
        return RC_PAGE_FULL;
    }
    memcpy(schemaPage + position, &schema->numAttr, sizeof(int));
    position += sizeof(int);
    
    // Write attribute names
    for (int i = 0; i < schema->numAttr; i++) {
        int nameLen = strlen(schema->attrNames[i]) + 1; // Include null terminator
        
        if (position + nameLen > PAGE_SIZE) {
            free(schemaPage);
            closePageFile(&fileHandle);
            return RC_PAGE_FULL;
        }
        
        memcpy(schemaPage + position, schema->attrNames[i], nameLen);
        position += nameLen;
    }
    
    // Write data types
    int dataTypesSize = schema->numAttr * sizeof(DataType);
    if (position + dataTypesSize > PAGE_SIZE) {
        free(schemaPage);
        closePageFile(&fileHandle);
        return RC_PAGE_FULL;
    }
    memcpy(schemaPage + position, schema->dataTypes, dataTypesSize);
    position += dataTypesSize;
    
    // Write type lengths
    int typeLengthsSize = schema->numAttr * sizeof(int);
    if (position + typeLengthsSize > PAGE_SIZE) {
        free(schemaPage);
        closePageFile(&fileHandle);
        return RC_PAGE_FULL;
    }
    memcpy(schemaPage + position, schema->typeLength, typeLengthsSize);
    position += typeLengthsSize;
    
    // Write key information
    if (position + sizeof(int) + (schema->keySize * sizeof(int)) > PAGE_SIZE) {
        free(schemaPage);
        closePageFile(&fileHandle);
        return RC_PAGE_FULL;
    }
    
    memcpy(schemaPage + position, &schema->keySize, sizeof(int));
    position += sizeof(int);
    
    memcpy(schemaPage + position, schema->keyAttrs, schema->keySize * sizeof(int));
    position += schema->keySize * sizeof(int);
    
    // Step 5: Write schema page to disk
    status = writeBlock(0, &fileHandle, schemaPage);
    free(schemaPage);
    
    if (status != RC_OK) {
        closePageFile(&fileHandle);
        return status;
    }
    
    // Step 6: Initialize page directory (page 1)
    char *directoryPage = calloc(1, PAGE_SIZE);
    if (!directoryPage) {
        closePageFile(&fileHandle);
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Set initial directory values
    int totalPages = 1;  // Start with one data page
    int directoryPages = 1;  // Start with one directory page
    
    // Write directory header
    position = 0;
    memcpy(directoryPage + position, &totalPages, sizeof(int));
    position += sizeof(int);
    
    memcpy(directoryPage + position, &directoryPages, sizeof(int));
    position += sizeof(int);
    
    // Initialize first page entry
    PageDirectoryEntry firstPage;
    initPageDirectoryEntry(&firstPage, 0);
    
    // Write the entry
    memcpy(directoryPage + position, &firstPage, sizeof(PageDirectoryEntry));
    
    // Step 7: Write directory page to disk
    status = writeBlock(1, &fileHandle, directoryPage);
    free(directoryPage);
    
    if (status != RC_OK) {
        closePageFile(&fileHandle);
        return status;
    }
    
    // Step 8: Close the file
    status = closePageFile(&fileHandle);
    if (status != RC_OK) {
        return status;
    }
    
    printf("Table '%s' created successfully\n", tableName);
    return RC_OK;
}

/* 
 * Helper function to initialize a page directory entry
 */
static void initPageDirectoryEntry(PageDirectoryEntry *entry, int pageId) {
    entry->pageID = pageId;
    entry->hasFreeSlot = true;
    entry->freeSpace = PAGE_SIZE;
    entry->recordCount = 0;
}

/* 
 * Opens an existing table and initializes the table data structure
 * This involves reading schema information and page directory
 */
RC openTable(RM_TableData *rel, char *tableName) {
    printf("Opening table '%s'...\n", tableName);
    
    // Validate input parameters
    if (!rel || !tableName) {
        printf("Error: Invalid table data or name\n");
        return RC_INVALID_INPUT;
    }
    
    // Step 1: Initialize table data structure
    rel->name = tableName;
    rel->schema = malloc(sizeof(Schema));
    if (!rel->schema) {
        printf("Error: Failed to allocate memory for schema\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    rel->managementData = malloc(sizeof(RM_managementData));
    if (!rel->managementData) {
        free(rel->schema);
        printf("Error: Failed to allocate memory for management data\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    RM_managementData *mgmtData = (RM_managementData *)rel->managementData;
    
    // Step 2: Open the page file
    RC status = openPageFile(tableName, &mgmtData->fileHndl);
    if (status != RC_OK) {
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to open page file for table '%s'\n", tableName);
        return status;
    }
    
    // Step 3: Initialize buffer pool
    status = initBufferPool(&mgmtData->bm, tableName, BUFFER_PAGE_LIMIT, RS_LRU, NULL);
    if (status != RC_OK) {
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to initialize buffer pool\n");
        return status;
    }
    
    // Step 4: Load schema information (page 0)
    status = pinPage(&mgmtData->bm, &mgmtData->pageHndlBM, 0);
    if (status != RC_OK) {
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to pin schema page\n");
        return status;
    }
    
    // Copy schema data to memory
    char *schemaData = malloc(PAGE_SIZE);
    if (!schemaData) {
        unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to allocate memory for schema data\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    memcpy(schemaData, mgmtData->pageHndlBM.data, PAGE_SIZE);
    
    // Unpin the schema page
    status = unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
    if (status != RC_OK) {
        free(schemaData);
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to unpin schema page\n");
        return status;
    }
    
    // Step 5: Parse schema information
    int position = 0;
    
    // Read number of attributes
    memcpy(&rel->schema->numAttr, schemaData + position, sizeof(int));
    position += sizeof(int);
    
    // Allocate memory for attribute names
    rel->schema->attrNames = malloc(rel->schema->numAttr * sizeof(char *));
    if (!rel->schema->attrNames) {
        free(schemaData);
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to allocate memory for attribute names\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Read attribute names
    for (int i = 0; i < rel->schema->numAttr; i++) {
        rel->schema->attrNames[i] = strdup(schemaData + position);
        position += strlen(schemaData + position) + 1;
    }
    
    // Allocate memory for data types
    rel->schema->dataTypes = malloc(rel->schema->numAttr * sizeof(DataType));
    if (!rel->schema->dataTypes) {
        for (int i = 0; i < rel->schema->numAttr; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->schema->attrNames);
        free(schemaData);
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to allocate memory for data types\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Read data types
    memcpy(rel->schema->dataTypes, schemaData + position, rel->schema->numAttr * sizeof(DataType));
    position += rel->schema->numAttr * sizeof(DataType);
    
    // Allocate memory for type lengths
    rel->schema->typeLength = malloc(rel->schema->numAttr * sizeof(int));
    if (!rel->schema->typeLength) {
        free(rel->schema->dataTypes);
        for (int i = 0; i < rel->schema->numAttr; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->schema->attrNames);
        free(schemaData);
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to allocate memory for type lengths\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Read type lengths
    memcpy(rel->schema->typeLength, schemaData + position, rel->schema->numAttr * sizeof(int));
    position += rel->schema->numAttr * sizeof(int);
    
    // Read key size
    memcpy(&rel->schema->keySize, schemaData + position, sizeof(int));
    position += sizeof(int);
    
    // Allocate memory for key attributes
    rel->schema->keyAttrs = malloc(rel->schema->keySize * sizeof(int));
    if (!rel->schema->keyAttrs) {
        free(rel->schema->typeLength);
        free(rel->schema->dataTypes);
        for (int i = 0; i < rel->schema->numAttr; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->schema->attrNames);
        free(schemaData);
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to allocate memory for key attributes\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Read key attributes
    memcpy(rel->schema->keyAttrs, schemaData + position, rel->schema->keySize * sizeof(int));
    
    // Free schema data
    free(schemaData);
    
    // Step 6: Load page directory (page 1)
    status = loadPageDirectoryFromDisk(rel);
    if (status != RC_OK) {
        free(rel->schema->keyAttrs);
        free(rel->schema->typeLength);
        free(rel->schema->dataTypes);
        for (int i = 0; i < rel->schema->numAttr; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->schema->attrNames);
        shutdownBufferPool(&mgmtData->bm);
        closePageFile(&mgmtData->fileHndl);
        free(rel->schema);
        free(rel->managementData);
        printf("Error: Failed to load page directory\n");
        return status;
    }
    
    printf("Table '%s' opened successfully\n", tableName);
    return RC_OK;
}

/* 
 * Helper function to load the page directory from disk
 */
static RC loadPageDirectoryFromDisk(RM_TableData *table) {
    RM_managementData *mgmtData = (RM_managementData *)table->managementData;
    
    // Pin the directory page (page 1)
    RC status = pinPage(&mgmtData->bm, &mgmtData->pageHndlBM, 1);
    if (status != RC_OK) {
        return status;
    }
    
    // Copy directory data to memory
    char *directoryData = malloc(PAGE_SIZE);
    if (!directoryData) {
        unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    memcpy(directoryData, mgmtData->pageHndlBM.data, PAGE_SIZE);
    
    // Unpin the directory page
    status = unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
    if (status != RC_OK) {
        free(directoryData);
        return status;
    }
    
    // Parse directory information
    int position = 0;
    
    // Read total pages and directory pages
    memcpy(&mgmtData->numPages, directoryData + position, sizeof(int));
    position += sizeof(int);
    
    memcpy(&mgmtData->numPageDP, directoryData + position, sizeof(int));
    position += sizeof(int);
    
    // Calculate number of entries
    int numEntries = mgmtData->numPages - mgmtData->numPageDP + 1;
    
    // Allocate memory for page directory
    mgmtData->pageDirectory = malloc(numEntries * sizeof(PageDirectoryEntry));
    if (!mgmtData->pageDirectory) {
        free(directoryData);
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Read page directory entries
    memcpy(mgmtData->pageDirectory, directoryData + position, numEntries * sizeof(PageDirectoryEntry));
    
    // Free directory data
    free(directoryData);
    
    return RC_OK;
}

/* 
 * Helper function to save the page directory to disk
 */
static RC savePageDirectoryToDisk(RM_TableData *table) {
    RM_managementData *mgmtData = (RM_managementData *)table->managementData;
    
    // Calculate max entries per directory page
    int maxEntriesPerPage = (PAGE_SIZE - 2 * sizeof(int)) / sizeof(PageDirectoryEntry);
    
    // Allocate memory for directory page
    char *directoryPage = calloc(1, PAGE_SIZE);
    if (!directoryPage) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Write directory header
    int position = 0;
    memcpy(directoryPage + position, &mgmtData->numPages, sizeof(int));
    position += sizeof(int);
    
    memcpy(directoryPage + position, &mgmtData->numPageDP, sizeof(int));
    position += sizeof(int);
    
    // Calculate number of entries
    int numEntries = mgmtData->numPages - mgmtData->numPageDP + 1;
    
    // Write page directory entries
    memcpy(directoryPage + position, mgmtData->pageDirectory, numEntries * sizeof(PageDirectoryEntry));
    
    // Calculate block to write
    int blockToWrite = (mgmtData->numPages / maxEntriesPerPage) * maxEntriesPerPage + mgmtData->numPageDP;
    
    // Write directory page to disk
    RC status = writeBlock(blockToWrite, &mgmtData->fileHndl, directoryPage);
    
    // Free directory page
    free(directoryPage);
    
    return status;
}

/* 
 * Closes a table and frees all associated resources
 */
RC closeTable(RM_TableData *rel) {
    printf("Closing table '%s'...\n", rel->name);
    
    // Validate input parameters
    if (!rel || !rel->managementData || !rel->schema) {
        printf("Warning: Table already closed or invalid\n");
        return RC_OK;
    }
    
    RM_managementData *mgmtData = (RM_managementData *)rel->managementData;
    
    // Step 1: Free schema information
    if (rel->schema->attrNames) {
        for (int i = 0; i < rel->schema->numAttr; i++) {
            if (rel->schema->attrNames[i]) {
                free(rel->schema->attrNames[i]);
            }
        }
        free(rel->schema->attrNames);
    }
    
    if (rel->schema->dataTypes) {
        free(rel->schema->dataTypes);
    }
    
    if (rel->schema->typeLength) {
        free(rel->schema->typeLength);
    }
    
    if (rel->schema->keyAttrs) {
        free(rel->schema->keyAttrs);
    }
    
    free(rel->schema);
    
    // Step 2: Free page directory
    if (mgmtData->pageDirectory) {
        free(mgmtData->pageDirectory);
    }
    
    // Step 3: Shutdown buffer pool
    RC status = shutdownBufferPool(&mgmtData->bm);
    if (status != RC_OK) {
        printf("Warning: Failed to shutdown buffer pool\n");
        // Continue with cleanup despite error
    }
    
    // Step 4: Close page file
    status = closePageFile(&mgmtData->fileHndl);
    if (status != RC_OK) {
        printf("Warning: Failed to close page file\n");
        // Continue with cleanup despite error
    }
    
    // Step 5: Free management data
    free(rel->managementData);
    
    printf("Table closed successfully\n");
    return RC_OK;
}

/* 
 * Deletes a table by removing its underlying page file
 */
RC deleteTable(char *tableName) {
    printf("Deleting table '%s'...\n", tableName);
    
    // Validate input parameters
    if (!tableName) {
        printf("Error: Invalid table name\n");
        return RC_INVALID_NAME;
    }
    
    // Delete the page file
    RC status = destroyPageFile(tableName);
    if (status != RC_OK) {
        printf("Error: Failed to delete page file for table '%s'\n", tableName);
        return status;
    }
    
    printf("Table '%s' deleted successfully\n", tableName);
    return RC_OK;
}

/* 
 * Returns the total number of records in the table
 */
int getNumTuples(RM_TableData *rel) {
    // Validate input parameters
    if (!rel || !rel->managementData) {
        return 0;
    }
    
    RM_managementData *mgmtData = (RM_managementData *)rel->managementData;
    
    // Count total records
    int totalRecords = 0;
    for (int i = 0; i < mgmtData->numPages - mgmtData->numPageDP + 1; i++) {
        totalRecords += mgmtData->pageDirectory[i].recordCount;
    }
    
    return totalRecords;
}

/*
 * Record Operations
 */

/* 
 * Inserts a new record into the table
 */
RC insertRecord(RM_TableData *rel, Record *record) {
    printf("Inserting record into table '%s'...\n", rel->name);
    
    // Validate input parameters
    if (!rel || !rel->managementData || !record) {
        printf("Error: Invalid table or record\n");
        return RC_INVALID_INPUT;
    }
    
    RM_managementData *mgmtData = (RM_managementData *)rel->managementData;
    
    // Calculate record size
    int recordSize = computeRecordSize(rel->schema);
    
    // Calculate max entries per directory page
    int maxEntriesPerPage = (PAGE_SIZE - 2 * sizeof(int)) / sizeof(PageDirectoryEntry);
    
    // Check if we need a new directory page
    bool needNewDirectory = mgmtData->numPages > maxEntriesPerPage * mgmtData->numPageDP;
    if (needNewDirectory) {
        // Increment page and directory counts
        mgmtData->numPages++;
        mgmtData->numPageDP++;
        
        // Create new directory page
        char *newDirectoryPage = calloc(1, PAGE_SIZE);
        if (!newDirectoryPage) {
            printf("Error: Failed to allocate memory for new directory page\n");
            return RC_MEMORY_ALLOCATION_FAIL;
        }
        
        // Write new directory page to disk
        RC status = appendEmptyBlock(&mgmtData->fileHndl);
        if (status != RC_OK) {
            free(newDirectoryPage);
            printf("Error: Failed to append new directory page\n");
            return status;
        }
        
        status = writeBlock(mgmtData->numPages, &mgmtData->fileHndl, newDirectoryPage);
        free(newDirectoryPage);
        
        if (status != RC_OK) {
            printf("Error: Failed to write new directory page\n");
            return status;
        }
    }
    
    // Find a page with free space
    int pageIndex = findFreePageIndex(rel);
    
    // If no free page found, create a new one
    if (pageIndex == -1) {
        // Calculate new page index
        pageIndex = mgmtData->numPages - mgmtData->numPageDP + 1;
        
        // Increment page count
        mgmtData->numPages++;
        
        // Resize page directory
        int newSize = (mgmtData->numPages - mgmtData->numPageDP + 1) * sizeof(PageDirectoryEntry);
        PageDirectoryEntry *newDirectory = realloc(mgmtData->pageDirectory, newSize);
        
        if (!newDirectory) {
            printf("Error: Failed to resize page directory\n");
            return RC_MEMORY_ALLOCATION_FAIL;
        }
        
        mgmtData->pageDirectory = newDirectory;
        
        // Initialize new page directory entry
        initPageDirectoryEntry(&mgmtData->pageDirectory[pageIndex], mgmtData->numPages - mgmtData->numPageDP);
        
        // Create new page
        char *newPage = calloc(1, PAGE_SIZE);
        if (!newPage) {
            printf("Error: Failed to allocate memory for new page\n");
            return RC_MEMORY_ALLOCATION_FAIL;
        }
        
        // Append and write new page to disk
        RC status = appendEmptyBlock(&mgmtData->fileHndl);
        if (status != RC_OK) {
            free(newPage);
            printf("Error: Failed to append new page\n");
            return status;
        }
        
        status = writeBlock(mgmtData->numPages, &mgmtData->fileHndl, newPage);
        free(newPage);
        
        if (status != RC_OK) {
            printf("Error: Failed to write new page\n");
            return status;
        }
    }
    
    // Pin the page - use the correct page number calculation
    int pageToPin = mgmtData->pageDirectory[pageIndex].pageID + mgmtData->numPageDP + 1;
    RC status = pinPage(&mgmtData->bm, &mgmtData->pageHndlBM, pageToPin);
    if (status != RC_OK) {
        printf("Error: Failed to pin page\n");
        return status;
    }
    
    // Get page data
    char *pageData = mgmtData->pageHndlBM.data;
    
    // Find a free slot in the page
    int slotIndex = locateFreeSlot(pageData, mgmtData->pageDirectory[pageIndex].recordCount);
    
    // If no free slot found, append at the end
    if (slotIndex == -1) {
        slotIndex = mgmtData->pageDirectory[pageIndex].recordCount++;
    }
    
    // Calculate record offset
    int recordOffset = PAGE_SIZE - (mgmtData->pageDirectory[pageIndex].recordCount * recordSize);
    
    // Update slot directory entry
    SlotDirectoryEntry *slotEntry = (SlotDirectoryEntry *)(pageData + slotIndex * sizeof(SlotDirectoryEntry));
    slotEntry->offset = recordOffset;
    slotEntry->isFree = false;
    
    // Copy record data to page
    memcpy(pageData + recordOffset, record->data, recordSize);
    
    // Update record ID
    record->id.page = mgmtData->pageDirectory[pageIndex].pageID;
    record->id.slot = slotIndex;
    
    // Update page statistics
    int spaceUsed = recordSize + sizeof(SlotDirectoryEntry);
    updatePageStatistics(rel, pageIndex, -spaceUsed, true);
    
    // Mark page as dirty
    status = markDirty(&mgmtData->bm, &mgmtData->pageHndlBM);
    if (status != RC_OK) {
        unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
        printf("Error: Failed to mark page as dirty\n");
        return status;
    }
    
    // Unpin the page
    status = unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
    if (status != RC_OK) {
        printf("Error: Failed to unpin page\n");
        return status;
    }
    
    // Save page directory to disk
    status = savePageDirectoryToDisk(rel);
    if (status != RC_OK) {
        printf("Error: Failed to save page directory\n");
        return status;
    }
    
    printf("Record inserted successfully\n");
    return RC_OK;
}

static int findFreePageIndex(RM_TableData *table) {
    RM_managementData *mgmtData = (RM_managementData *)table->managementData;
    
    for (int i = 0; i < mgmtData->numPages - mgmtData->numPageDP + 1; i++) {
        if (mgmtData->pageDirectory[i].hasFreeSlot) {
            return i;  // Found a page with free space
        }
    }
    
    return -1;  // No page with free space found
}

/* 
 * Helper function to find a free slot in a page
 */
static int locateFreeSlot(char *pageData, int recordCount) {
    for (int i = 0; i < recordCount; i++) {
        SlotDirectoryEntry *slotEntry = (SlotDirectoryEntry *)(pageData + i * sizeof(SlotDirectoryEntry));
        if (slotEntry->isFree) {
            return i;  // Found a free slot
        }
    }
    
    return -1;  // No free slot found
}

/* 
 * Helper function to update page statistics
 */
static void updatePageStatistics(RM_TableData *table, int pageIdx, int spaceChange, bool recordAdded) {
    RM_managementData *mgmtData = (RM_managementData *)table->managementData;
    
    // Update free space
    mgmtData->pageDirectory[pageIdx].freeSpace += spaceChange;
    
    // Check if page still has free slots
    int recordSize = computeRecordSize(table->schema);
    bool hasSpace = mgmtData->pageDirectory[pageIdx].freeSpace >= (recordSize + sizeof(SlotDirectoryEntry));
    mgmtData->pageDirectory[pageIdx].hasFreeSlot = hasSpace;
}

/* 
 * Deletes a record from the table
 */
RC deleteRecord(RM_TableData *rel, RID id) {
    printf("Deleting record from table '%s'...\n", rel->name);
    
    // Validate input parameters
    if (!rel || !rel->managementData) {
        printf("Error: Invalid table\n");
        return RC_INVALID_INPUT;
    }
    
    RM_managementData *mgmtData = (RM_managementData *)rel->managementData;
    
    // Validate RID
    if (!isValidRecordID(id, mgmtData->numPages)) {
        printf("Error: Invalid record ID\n");
        return RC_RM_INVALID_RID;
    }
    
    // Pin the page
    RC status = pinPage(&mgmtData->bm, &mgmtData->pageHndlBM, id.page + mgmtData->numPageDP + 1);
    if (status != RC_OK) {
        printf("Error: Failed to pin page\n");
        return status;
    }
    
    // Get page data
    char *pageData = mgmtData->pageHndlBM.data;
    
    // Get slot entry
    SlotDirectoryEntry *slotEntry = (SlotDirectoryEntry *)(pageData + id.slot * sizeof(SlotDirectoryEntry));
    
    // Check if slot is already free
    if (slotEntry->isFree) {
        unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
        printf("Error: Record not found\n");
        return RC_RM_RECORD_NOT_FOUND;
    }
    
    // Mark slot as free
    slotEntry->isFree = true;
    
    // Mark the first byte of the record as deleted (tombstone)
    pageData[slotEntry->offset] = DELETED_RECORD_MARKER;
    
    // Update page statistics
    int recordSize = computeRecordSize(rel->schema);
    updatePageStatistics(rel, id.page, recordSize, false);
    
    // Mark page as dirty
    status = markDirty(&mgmtData->bm, &mgmtData->pageHndlBM);
    if (status != RC_OK) {
        unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
        printf("Error: Failed to mark page as dirty\n");
        return status;
    }
    
    // Unpin the page
    status = unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
    if (status != RC_OK) {
        printf("Error: Failed to unpin page\n");
        return status;
    }
    
    // Save page directory to disk
    status = savePageDirectoryToDisk(rel);
    if (status != RC_OK) {
        printf("Error: Failed to save page directory\n");
        return status;
    }
    
    printf("Record deleted successfully\n");
    return RC_OK;
}

/* 
 * Helper function to check if a RID is valid
 */
static bool isValidRecordID(RID id, int totalPages) {
    return (id.page >= 0 && id.page < totalPages && id.slot >= 0);
}

RC updateRecord(RM_TableData *table, Record *record) {
    printf("Attempting to modify record in table: %s\n", table->name);
    
    if (!table || !table->managementData || !record) {
        fprintf(stderr, "Error: Null reference detected for table or record\n");
        return RC_INVALID_INPUT;
    }
    
    RM_managementData *metadata = (RM_managementData *)table->managementData;
    
    if (!isValidRecordID(record->id, metadata->numPages)) {
        fprintf(stderr, "Error: Provided Record ID is invalid\n");
        return RC_RM_INVALID_RID;
    }
    
    RC result = pinPage(&metadata->bm, &metadata->pageHndlBM, record->id.page + metadata->numPageDP + 1);
    if (result != RC_OK) {
        fprintf(stderr, "Failure in pinning the page\n");
        return result;
    }
    
    char *pageBuffer = metadata->pageHndlBM.data;
    SlotDirectoryEntry *slotInfo = (SlotDirectoryEntry *)(pageBuffer + record->id.slot * sizeof(SlotDirectoryEntry));
    
    if (slotInfo->isFree) {
        unpinPage(&metadata->bm, &metadata->pageHndlBM);
        fprintf(stderr, "Error: No valid record found at the specified location\n");
        return RC_RM_RECORD_NOT_FOUND;
    }
    
    int requiredSize = computeRecordSize(table->schema);
    int spaceAvailable = metadata->pageDirectory[record->id.page].freeSpace + 
                         (slotInfo->offset - (record->id.slot * sizeof(SlotDirectoryEntry)));
    
    if (requiredSize > spaceAvailable) {
        unpinPage(&metadata->bm, &metadata->pageHndlBM);
        
        if ((result = deleteRecord(table, record->id)) != RC_OK) {
            fprintf(stderr, "Error: Deletion process failed during update\n");
            return result;
        }
        
        if ((result = insertRecord(table, record)) != RC_OK) {
            fprintf(stderr, "Error: Reinsertion failed during update\n");
            return result;
        }
    } else {
        memcpy(pageBuffer + slotInfo->offset, record->data, requiredSize);
        
        if ((result = markDirty(&metadata->bm, &metadata->pageHndlBM)) != RC_OK) {
            unpinPage(&metadata->bm, &metadata->pageHndlBM);
            fprintf(stderr, "Error: Failed to flag page as modified\n");
            return result;
        }
        
        if ((result = unpinPage(&metadata->bm, &metadata->pageHndlBM)) != RC_OK) {
            fprintf(stderr, "Error: Issue encountered during page unpinning\n");
            return result;
        }
    }
    
    printf("Record update finalized successfully\n");
    return RC_OK;
}


/* 
 * Retrieves a record from the table
 */
RC getRecord(RM_TableData *rel, RID id, Record *record) {
    // Validate input parameters
    if (!rel || !rel->managementData || !record) {
        return RC_INVALID_INPUT;
    }
    
    RM_managementData *mgmtData = (RM_managementData *)rel->managementData;
    
    // Validate RID
    if (!isValidRecordID(id, mgmtData->numPages)) {
        return RC_RM_INVALID_RID;
    }
    
    // Pin the page
    RC status = pinPage(&mgmtData->bm, &mgmtData->pageHndlBM, id.page + mgmtData->numPageDP + 1);
    if (status != RC_OK) {
        return status;
    }
    
    // Get page data
    char *pageData = mgmtData->pageHndlBM.data;
    
    // Get slot entry
    SlotDirectoryEntry *slotEntry = (SlotDirectoryEntry *)(pageData + id.slot * sizeof(SlotDirectoryEntry));
    
    // Check if slot is free
    if (slotEntry->isFree) {
        unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
        return RC_RM_RECORD_NOT_FOUND;
    }
    
    // Set record ID
    record->id = id;
    
    // Calculate record size
    int recordSize = computeRecordSize(rel->schema);
    
    // Allocate memory for record data if needed
    if (record->data == NULL) {
        record->data = malloc(recordSize);
        if (record->data == NULL) {
            unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
            return RC_MEMORY_ALLOCATION_FAIL;
        }
    }
    
    // Copy record data
    memcpy(record->data, pageData + slotEntry->offset, recordSize);
    
    // Unpin the page
    status = unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
    
    return status;
}

/*
 * Scan Operations
 */

/* 
 * Initializes a scan operation on the table
 */
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *condition) {
    printf("Starting scan on table '%s'...\n", rel->name);
    
    // Validate input parameters
    if (!rel || !scan) {
        printf("Error: Invalid table or scan handle\n");
        return RC_INVALID_INPUT;
    }
    
    // Initialize scan handle
    scan->rel = rel;
    
    // Allocate memory for scan info
    ScanInfo *scanInfo = malloc(sizeof(ScanInfo));
    if (!scanInfo) {
        printf("Error: Failed to allocate memory for scan info\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Initialize scan info
    scanInfo->condition = condition;
    scanInfo->currentPage = 0;
    scanInfo->currentSlot = 0;
    
    // Set scan management data
    scan->mgmtData = scanInfo;
    
    printf("Scan initialized successfully\n");
    return RC_OK;
}

/* 
 * Helper function for ceiling division
 */
static int ceilDivision(int numerator, int denominator) {
    return (numerator + denominator - 1) / denominator;
}

/* 
 * Retrieves the next record that satisfies the scan condition
 */
RC next(RM_ScanHandle *scan, Record *record) {
    // Validate input parameters
    if (!scan || !scan->rel || !scan->mgmtData || !record) {
        return RC_INVALID_INPUT;
    }
    
    ScanInfo *scanInfo = (ScanInfo *)scan->mgmtData;
    RM_TableData *rel = scan->rel;
    RM_managementData *mgmtData = (RM_managementData *)rel->managementData;
    
    // Calculate max entries per directory page
    int maxEntriesPerPage = (PAGE_SIZE - 2 * sizeof(int)) / sizeof(PageDirectoryEntry);
    
    // Calculate record size
    int recordSize = computeRecordSize(rel->schema);
    
    // Scan through pages
    for (; scanInfo->currentPage < mgmtData->numPages - mgmtData->numPageDP + 1; scanInfo->currentPage++) {
        // Calculate page number to pin
        int pageNum = ceilDivision(scanInfo->currentPage + 1, maxEntriesPerPage) + 1 + scanInfo->currentPage;
        
        // Pin the page
        RC status = pinPage(&mgmtData->bm, &mgmtData->pageHndlBM, pageNum);
        if (status != RC_OK) {
            return status;
        }
        
        // Get page data
        char *pageData = mgmtData->pageHndlBM.data;
        
        // Scan through slots
        for (; scanInfo->currentSlot < mgmtData->pageDirectory[scanInfo->currentPage].recordCount; scanInfo->currentSlot++) {
            // Get slot entry
            SlotDirectoryEntry *slotEntry = (SlotDirectoryEntry *)(pageData + scanInfo->currentSlot * sizeof(SlotDirectoryEntry));
            
            // Skip free slots
            if (slotEntry->isFree) {
                continue;
            }
            
            // Set record ID
            record->id.page = scanInfo->currentPage;
            record->id.slot = scanInfo->currentSlot;
            
            // Allocate memory for record data if needed
            if (record->data == NULL) {
                record->data = malloc(recordSize);
                if (record->data == NULL) {
                    unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
                    return RC_MEMORY_ALLOCATION_FAIL;
                }
            }
            
            // Copy record data
            memcpy(record->data, pageData + slotEntry->offset, recordSize);
            
            // Evaluate condition
            Value *result = NULL;
            evalExpr(record, rel->schema, scanInfo->condition, &result);
            
            // Check if condition is satisfied
            bool conditionMet = (result->v.boolV == TRUE) || (scanInfo->condition == NULL);
            
            // Free result
            freeVal(result);
            
            if (conditionMet) {
                // Increment slot for next call
                scanInfo->currentSlot++;
                
                // Unpin the page
                unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
                
                return RC_OK;
            }
        }
        
        // Reset slot for next page
        scanInfo->currentSlot = 0;
        
        // Unpin the page
        unpinPage(&mgmtData->bm, &mgmtData->pageHndlBM);
    }
    
    // No more records
    return RC_RM_NO_MORE_TUPLES;
}

/* 
 * Closes a scan operation
 */
RC closeScan(RM_ScanHandle *scan) {
    printf("Closing scan...\n");
    
    // Validate input parameters
    if (!scan || !scan->mgmtData) {
        printf("Warning: Scan already closed or invalid\n");
        return RC_OK;
    }
    
    // Free scan info
    free(scan->mgmtData);
    scan->mgmtData = NULL;
    
    printf("Scan closed successfully\n");
    return RC_OK;
}

/*
 * Schema and Record Operations
 */

/* 
 * Calculates the size of records for a given schema
 */
static int computeRecordSize(Schema *schema) {
    int size = 0;
    
    for (int i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                size += sizeof(int);
                break;
            case DT_FLOAT:
                size += sizeof(float);
                break;
            case DT_BOOL:
                size += sizeof(bool);
                break;
            case DT_STRING:
                size += schema->typeLength[i];
                break;
            default:
                // Unsupported data type
                break;
        }
    }
    
    return size;
}

/* 
 * Returns the size of records for a given schema (public interface)
 */
int getRecordSize(Schema *schema) {
    return computeRecordSize(schema);
}

Schema *createSchema(int attributeCount, char **attributeNames, DataType *dataTypes, int *lengths, int keyCount, int *keyAttributes) {
    printf("Initializing schema with %d attributes...\n", attributeCount);
    
    Schema *schemaInstance = (Schema *)malloc(sizeof(Schema));
    if (!schemaInstance) {
        perror("Memory allocation failed for schema");
        return NULL;
    }
    
    schemaInstance->numAttr = attributeCount;
    schemaInstance->keySize = keyCount;
    
    // Allocate arrays
    schemaInstance->attrNames = (char **)malloc(attributeCount * sizeof(char *));
    schemaInstance->dataTypes = (DataType *)malloc(attributeCount * sizeof(DataType));
    schemaInstance->typeLength = (int *)malloc(attributeCount * sizeof(int));
    schemaInstance->keyAttrs = (int *)malloc(keyCount * sizeof(int));
    
    // Check allocations
    if (!schemaInstance->attrNames || !schemaInstance->dataTypes || !schemaInstance->typeLength || !schemaInstance->keyAttrs) {
        perror("Memory allocation failed for schema components");
        free(schemaInstance->attrNames);
        free(schemaInstance->dataTypes);
        free(schemaInstance->typeLength);
        free(schemaInstance->keyAttrs);
        free(schemaInstance);
        return NULL;
    }
    
    // Copy attribute names and details
    for (int i = 0; i < attributeCount; i++) {
        schemaInstance->attrNames[i] = strdup(attributeNames[i]);
        if (!schemaInstance->attrNames[i]) {
            perror("Memory allocation failed for attribute name");
            while (i--) free(schemaInstance->attrNames[i]);
            free(schemaInstance->attrNames);
            free(schemaInstance->dataTypes);
            free(schemaInstance->typeLength);
            free(schemaInstance->keyAttrs);
            free(schemaInstance);
            return NULL;
        }
        schemaInstance->dataTypes[i] = dataTypes[i];
        schemaInstance->typeLength[i] = lengths[i];
    }
    
    // Copy key attributes
    memcpy(schemaInstance->keyAttrs, keyAttributes, keyCount * sizeof(int));
    
    printf("Schema successfully initialized.\n");
    return schemaInstance;
}


/* 
 * Frees memory allocated for a schema
 */
RC freeSchema(Schema *schema) {
    printf("Freeing schema...\n");
    
    // Validate input parameters
    if (!schema) {
        printf("Warning: Schema already freed or invalid\n");
        return RC_OK;
    }
    
    // Free attribute names
    if (schema->attrNames) {
        for (int i = 0; i < schema->numAttr; i++) {
            if (schema->attrNames[i]) {
                free(schema->attrNames[i]);
            }
        }
        free(schema->attrNames);
    }
    
    // Free data types
    if (schema->dataTypes) {
        free(schema->dataTypes);
    }
    
    // Free type lengths
    if (schema->typeLength) {
        free(schema->typeLength);
    }
    
    // Free key attributes
    if (schema->keyAttrs) {
        free(schema->keyAttrs);
    }
    
    // Free schema
    free(schema);
    
    printf("Schema freed successfully\n");
    return RC_OK;
}

/* 
 * Creates a new record for a given schema
 */
RC createRecord(Record **record, Schema *schema) {
    printf("Creating record...\n");
    
    // Validate input parameters
    if (!record || !schema) {
        printf("Error: Invalid record pointer or schema\n");
        return RC_INVALID_INPUT;
    }
    
    // Allocate memory for record
    Record *newRecord = malloc(sizeof(Record));
    if (!newRecord) {
        printf("Error: Failed to allocate memory for record\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Initialize record ID
    newRecord->id.page = INVALID_PAGE_NUM;
    newRecord->id.slot = INVALID_SLOT_NUM;
    
    // Calculate record size
    int recordSize = computeRecordSize(schema);
    
    // Allocate memory for record data
    newRecord->data = calloc(1, recordSize);
    if (!newRecord->data) {
        printf("Error: Failed to allocate memory for record data\n");
        free(newRecord);
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Set output parameter
    *record = newRecord;
    
    printf("Record created successfully\n");
    return RC_OK;
}

/* 
 * Frees memory allocated for a record
 */
RC freeRecord(Record *record) {
    printf("Freeing record...\n");
    
    // Validate input parameters
    if (!record) {
        printf("Warning: Record already freed or invalid\n");
        return RC_OK;
    }
    
    // Free record data
    if (record->data) {
        free(record->data);
    }
    
    // Free record
    free(record);
    
    printf("Record freed successfully\n");
    return RC_OK;
}

/* 
 * Helper function to calculate the offset of an attribute in a record
 */
static int calculateAttributeOffset(Schema *schema, int attrIdx) {
    int offset = 0;
    
    for (int i = 0; i < attrIdx; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema->typeLength[i];
                break;
            default:
                // Unsupported data type
                break;
        }
    }
    
    return offset;
}

/* 
 * Sets the value of an attribute in a record
 */
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    // Validate input parameters
    if (!record || !schema || !value) {
        return RC_INVALID_INPUT;
    }
    
    // Validate attribute number
    if (attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_RM_INVALID_ATTRIBUTE;
    }
    
    // Validate data type
    if (value->dt != schema->dataTypes[attrNum]) {
        return RC_RM_ATTRIBUTE_TYPE_MISMATCH;
    }
    
    // Calculate attribute offset
    int offset = calculateAttributeOffset(schema, attrNum);
    
    // Set attribute value
    switch (value->dt) {
        case DT_INT:
            memcpy(record->data + offset, &value->v.intV, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(record->data + offset, &value->v.floatV, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(record->data + offset, &value->v.boolV, sizeof(bool));
            break;
        case DT_STRING:
            strncpy(record->data + offset, value->v.stringV, schema->typeLength[attrNum]);
            break;
        default:
            return RC_RM_DATA_TYPE_ERROR;
    }
    
    return RC_OK;
}

/* 
 * Gets the value of an attribute from a record
 */
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    // Validate input parameters
    if (!record || !schema || !value) {
        return RC_INVALID_INPUT;
    }
    
    // Validate attribute number
    if (attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_RM_INVALID_ATTRIBUTE;
    }
    
    // Calculate attribute offset
    int offset = calculateAttributeOffset(schema, attrNum);
    
    // Allocate memory for value
    *value = malloc(sizeof(Value));
    if (!*value) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }
    
    // Set data type
    (*value)->dt = schema->dataTypes[attrNum];
    
    // Get attribute value
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(&(*value)->v.intV, record->data + offset, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(&(*value)->v.floatV, record->data + offset, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(&(*value)->v.boolV, record->data + offset, sizeof(bool));
            break;
        case DT_STRING:
            (*value)->v.stringV = malloc(schema->typeLength[attrNum] + 1);
            if (!(*value)->v.stringV) {
                free(*value);
                return RC_MEMORY_ALLOCATION_FAIL;
            }
            
            strncpy((*value)->v.stringV, record->data + offset, schema->typeLength[attrNum]);
            (*value)->v.stringV[schema->typeLength[attrNum]] = '\0';
            break;
        default:
            free(*value);
            return RC_RM_DATA_TYPE_ERROR;
    }
    
    return RC_OK;
}