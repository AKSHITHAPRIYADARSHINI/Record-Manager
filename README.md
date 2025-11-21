#  Record Manager

The **Record Manager** is responsible for organizing and managing records within a table. It provides essential functionality for creating, opening, and closing tables, ensuring smooth data handling. Additionally, it facilitates managing individual records, allowing for efficient retrieval, modification, and deletion. The Record Manager also includes features for scanning records based on specific conditions, enabling targeted data access. Furthermore, it supports schema management by allowing updates to table structures and handling attribute values effectively. Overall, the Record Manager plays a crucial role in maintaining and manipulating structured data efficiently in C-based applications.


## FILES INCLUDED IN THE ASSIGNMENT
- README.md
- buffer_mgr_stat.c
- buffer_mgr_stat.h
- buffer_mgr.c
- buffer_mgr.h
- dberror.c
- dberror.h
- dt.h
- Makefile
- storage_mgr.c
- storage_mgr.h
- test_assign3_1.c
- test_helper.h
- expr.c
- expr.h
- record_mgr.h
- rm_serializer.c
- tables.h

## HOW TO RUN THE ASSIGNMENT
Step 1: Open the terminal and go to the project folder (assign3). cd
Step 2: Clean previous compiled files: make clean
Step 3: Compile the project files: make
Step 4: Run the main test file to check core record manager functionality: make run

## Functions
The following functions were created to implement the record manager:
### TABLE AND RECORD MANAGER FUNCTIONS

1.	initRecordManager (...)
- This function sets up the record manager using the storage manager.
- It prepares the record manager to perform table and record operations.

2.	shutdownRecordManager()
- This function shuts down the record manager and frees up the memory used.
- It ensures all active tables are closed and any allocated resources are released.

3.	createTable(...)
- This function creates a new table with the given name and schema.
- It initializes a new page file for the table and stores metadata such as the schema.
- The buffer pool is also set up to manage the table's pages efficiently.

4.	openTable (...)
- This function opens a table by reading its schema from a file and setting up the buffer pool.
- It prepares the table for record insertion, deletion, and scanning operations.

5.	closeTable(...)
- This function closes the table and saves any changes to the page file.
- It ensures any buffered changes are written to the page file and frees associated resources.

6.	deleteTable(...)
- This function deletes a table by removing its page file from the disk.
- It releases the storage space occupied by the table and its metadata.

7.	getNumTuples():
- This function returns the number of records (tuples) in a table.
- It fetches and returns the count of valid records stored in the table.

### RECORD HANDLING FUNCTIONS:
1.	insertRecord(...)
- This function inserts a new record into the table.
- It locates a free slot on a page, writes the record, and marks the page as dirty to indicate modification.
- The record is assigned a unique Record ID (RID) composed of page and slot numbers.

2.	deleteRecord(...)
- This function deletes a record using its record ID.
- It marks the record as deleted by updating its status.
- The space occupied by the deleted record is made available for future inserts.

3.	updateRecord(...)
- This function updates an existing record with new data.
- It finds the record by its RID, replaces its content, and marks the page as modified.
- The updated data is stored back in the same location.

4.	getRecord(....)
- This function retrieves a record from the table using its record ID.
- It finds the record's location in the page file and returns its data to the caller.

### SCAN FUNCTIONS:
1.	startScan (...)
- This function starts scanning records in a table.
- It can filter records using a condition (expression).
- If no condition is provided, all records are returned during the scan.

2.	next(...)
- This function returns the next record that meets the search condition.
- It iterates through the table, evaluates each record against the condition, and returns matching records.
- If no records are left, it returns an error code (RC_RM_NO_MORE_TUPLES).

3.	closeScan(...)
- This function ends the scanning process and frees the resources used during the scan.
- It resets the scan state and releases any occupied memory.

### SCHEMA FUNCTIONS:
1.	getRecordSize(...)
- This function calculates and returns the size of a record based on its schema.
- It adds the size of each attribute to determine the total record size.

2.	freeSchema(...)
- This function frees the memory used by a schema.
- It releases all dynamically allocated structures linked to the schema.

3.	createSchema(...)
- This function creates a new schema with the provided attribute names, types, and lengths.
- It defines the structure of records for a specific table.

Link of the video: https://vimeo.com/1066056982/83aa406c2a?ts=0&share=copy