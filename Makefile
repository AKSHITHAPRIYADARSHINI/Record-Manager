# Define the compiler
CC = gcc

# Define compiler flags
CFLAGS = -I.

# Define the source files
SRC = test_assign3_1.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c record_mgr.c expr.c rm_serializer.c dberror.c

# Define the header files (for dependency tracking)
HEADERS = buffer_mgr.h buffer_mgr_stat.h storage_mgr.h dt.h test_helper.h record_mgr.h expr.h tables.h

# Define the object files
OBJS = $(SRC:.c=.o)

# Define the target executable
TARGET = test_assign3_1

# Default target will be "all"
all: $(TARGET)

# Rule to build the target executable
$(TARGET): $(OBJS)
	$(CC) -o $(TARGET) $(OBJS)

# Rule to compile source files into object files
%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

# Clean rule to remove build artifacts
clean:
	rm -rf *.o $(TARGET) *.bin test_assign3_1

# Rule to run the executable
.PHONY: run
run: $(TARGET)
	./$(TARGET)
