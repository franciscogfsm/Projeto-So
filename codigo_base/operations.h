#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE],int fd_out);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE],int fd_out);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int fd_out);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(const char *full_path,const char *buffer,int file_bcks);

void format_Char(char *backup_filename, size_t size, const char *full_path, const char *buffer, int file_bcks);

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

/// Locks specific lines in the hash table based on the provided keys. It can 
/// read or write lock based on the write parameter.
/// @param num_pairs Number of key-value pairs.
/// @param keys Array of keys for which to acquire locks.
/// @param write if 1 acquires write locks; otherwise, acquires read locks.
void line_locker(size_t num_pairs,char keys[][MAX_STRING_SIZE],int write);

/// Releases locks specific lines in the hash table based on the provided keys. It can
/// read or write lock based on the write parameter.
/// @param num_pairs Number of key-value pairs.
/// @param keys Array of keys for which to release locks.
void line_unlocker(size_t num_pairs,char keys[][MAX_STRING_SIZE]);

///Compares two keys.
/// @param a Pointer to the first key.
/// @param b Pointer to the second key.
/// @return An integer less than, equal to, or greater than zero if the first argument 
/// is considered to be respectively less than, equal to, or greater than the second.
int compare_keys(const void *a, const void *b);

/// Acquires read locks for all lines in the hash table.
void global_line_locker();

/// Releases all locks for all lines in the hash table.
void global_line_unlocker();


#endif  // KVS_OPERATIONS_H
