#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>
#include "constants.h"

typedef struct {
    int client_fd;  // Client file descriptor.
} QueueNode;

typedef struct {
    QueueNode *nodes;     // Array of queue nodes.
    size_t capacity;      // Maximum size of the queue.
    size_t size;          // Current number of elements in the queue.
    size_t front;         // Index of the front element.
    size_t rear;          // Index of the rear element.
} Queue;

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
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int fd);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(size_t num_backup, char *job_filename, char *directory);

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

// Setter for max_backups
// @param _max_backups
void set_max_backups(int _max_backups);

// Setter for n_current_backups
// @param _n_current_backups
void set_n_current_backups(int _n_current_backups);

// Getter for n_current_backups
// @return n_current_backups
int get_n_current_backups();

/// Queue-related functions.
/// Initializes a queue with the given capacity.
/// @param queue Pointer to the queue.
/// @param capacity Maximum number of elements in the queue.
/// @return 0 if the queue was initialized successfully, 1 otherwise.
int queue_init(Queue *queue, size_t capacity);

/// Enqueues a client request.
/// @param queue Pointer to the queue.
/// @param client_fd Client file descriptor.
/// @return 0 if the client was enqueued successfully, 1 otherwise.
int queue_enqueue(Queue *queue, int client_fd);

/// Dequeues a client request.
/// @param queue Pointer to the queue.
/// @param client_fd Pointer to store the dequeued client file descriptor.
/// @return 0 if a client was dequeued successfully, 1 otherwise.
int queue_dequeue(Queue *queue, int *client_fd);

/// Destroys the queue, freeing any allocated memory.
/// @param queue Pointer to the queue.
void queue_destroy(Queue *queue);




#endif // KVS_OPERATIONS_H
