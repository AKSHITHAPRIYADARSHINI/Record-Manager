#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 128

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_READ_FAILED 5
#define RC_DELETE_FAILED 6
#define RC_FILE_OPEN_FAILED 7
#define RC_FILE_CLOSE_FAILED 8
#define RC_MALLOC_ERROR 9
#define RC_INVALID_INPUT 10
#define RC_MEMORY_ALLOCATION_FAIL 11
#define RC_PAGE_FULL 12
#define RC_RM_NULL_VALUE 13
#define RC_RM_STRING_TOO_LONG 14
#define RC_RM_DATA_SIZE_ERROR 15
#define RC_RM_NULL_POINTER 16
#define RC_INVALID_NAME 17
#define RC_MEM_ALLOCATION_FAIL 18
#define RC_RM_SCAN_INFO_NULL 19
#define RC_RM_INVALID_RECORD_SIZE 20
#define RC_MEMORY_ALLOCATION_ERROR 21
#define RC_NO_FREE_SLOT_FOUND 22
#define RC_UNEXPECTED_ACTION 25

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

#define RC_BP_INIT_ERROR 401
#define RC_BP_SHUNTDOWN_ERROR 402
#define RC_BP_FLUSHPOOL_FAILED 403
#define RC_BP_PIN_ERROR 404
#define RC_BP_UNPIN_ERROR 405
#define RC_BP_UNMARK_ERROR 406
#define RC_BP_FORCE_ERROR 407

#define RC_RM_TABLE_ERROR 501
#define RC_RM_NO_SLOT_ERROR 502
#define RC_RM_SCAN_HANDLE_ERROR 503
#define RC_RM_DATA_TYPE_ERROR 504
#define RC_RM_INVALID_RID 505
#define RC_RM_RECORD_NOT_FOUND 506
#define RC_RM_INVALID_ATTRIBUTE 507
#define RC_RM_ATTRIBUTE_TYPE_MISMATCH 508
#define RC_RM_NO_MORE_SPACE 509
#define RC_RM_MALLOC_FAILED 510

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
		do {			  \
			RC_message=message;	  \
			return rc;		  \
		} while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
		do {									\
			int rc_internal = (code);						\
			if (rc_internal != RC_OK)						\
			{									\
				char *message = errorMessage(rc_internal);			\
				printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
				free(message);							\
				exit(1);							\
			}									\
		} while(0);


#endif
