#ifndef TABLES_H
#define TABLES_H

#include "dt.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"

// Data Types, Records, and Schemas
typedef enum DataType {
	DT_INT = 0,
	DT_STRING = 1,
	DT_FLOAT = 2,
	DT_BOOL = 3
} DataType;

typedef struct Value {
	DataType dt;
	union v {
		int intV;
		char *stringV;
		float floatV;
		bool boolV;
	} v;
} Value;

typedef struct SlotDirectoryEntry {
    int offset;
    bool isFree;
} SlotDirectoryEntry;

typedef struct RID {
	int page;
	int slot;
} RID;

typedef struct Record
{
	RID id;
	char *data;
} Record;

typedef struct PageDirectoryEntry {
    int pageID;
    bool hasFreeSlot; // a flag indicating if there are free slots on the page
    int freeSpace;  // amount of free space available on the page
    int recordCount; // currently record numbers
} PageDirectoryEntry;

// information of the management data
typedef struct RM_managementData
{
    SM_FileHandle fileHndl;
    SM_PageHandle memPageSM;
    BM_BufferPool bm;
    BM_PageHandle pageHndlBM;
    PageDirectoryEntry *pageDirectory; // Added field for page directory
    int numPages; // Added field for number of pages
    int numPageDP;
} RM_managementData;

// information of a table schema: its attributes, datatypes, 
typedef struct Schema
{
	int numAttr;
	char **attrNames;
	DataType *dataTypes;
	int *typeLength;
	int *keyAttrs;
	int keySize;
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData
{
	char *name;
	Schema *schema;
	void *managementData;
} RM_TableData;

#define MAKE_STRING_VALUE(result, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = DT_STRING;						\
			(result)->v.stringV = (char *) malloc(strlen(value) + 1);		\
			strcpy((result)->v.stringV, value);					\
		} while(0)


#define MAKE_VALUE(result, datatype, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = datatype;						\
			switch(datatype)							\
			{									\
			case DT_INT:							\
			(result)->v.intV = value;					\
			break;								\
			case DT_FLOAT:							\
			(result)->v.floatV = value;					\
			break;								\
			case DT_BOOL:							\
			(result)->v.boolV = value;					\
			break;								\
			}									\
		} while(0)


// debug and read methods
extern Value *stringToValue (char *value);
extern char *serializeTableInfo(RM_TableData *rel);
extern char *serializeTableContent(RM_TableData *rel);
extern char *serializeSchema(Schema *schema);
extern char *serializeRecord(Record *record, Schema *schema);
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
extern char *serializeValue(Value *val);

#endif
