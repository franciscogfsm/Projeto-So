#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H
#define TABLE_SIZE 26
#include <pthread.h>
#include <stddef.h>
#include "src/server/constants.h"
#include <stdbool.h>


typedef struct KeyNode {
  char *key;
  char *value;
  struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
  KeyNode *table[TABLE_SIZE];
  pthread_rwlock_t tablelock;
} HashTable;



typedef struct ClientNode {
    int client_fd;                 // Client identifier
    char notif_pipe[MAX_STRING_SIZE];
    struct ClientNode *next;        // Next client
} ClientNode;

typedef struct SubscriptionNode {
    char key[MAX_STRING_SIZE];         // Name of the key
    ClientNode *clients;               // List of subscribed clients
    struct SubscriptionNode *next;     // Next key (in case of collision)
} SubscriptionNode;

typedef struct ClientTable {
    SubscriptionNode *table[TABLE_SIZE];  // Array of SubscriptionNodes
    pthread_rwlock_t lock;               // Protects read/write operations
} ClientTable;

extern ClientTable *subscription_table;



/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

int hash(const char *key);

// Writes a key value pair in the hash table.
// @param ht The hash table.
// @param key The key.
// @param value The value.
// @return 0 if successful.
int write_pair(HashTable *ht, const char *key, const char *value);

// Reads the value of a given key.
// @param ht The hash table.
// @param key The key.
// return the value if found, NULL otherwise.
char *read_pair(HashTable *ht, const char *key);

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);


bool key_exists(HashTable *ht, const char *key);



ClientTable *create_client_table();
void free_client_table(ClientTable *table);
unsigned int hash_function(const char *key);
int add_subscription(ClientTable *table, const char *key, int client_fd, const char *notif_pipe);
int remove_subscription(ClientTable *table, const char *key, int client_fd);
int subscribe_client(ClientTable *table, int client_fd, const char *key, const char *notif_pipe);
int subscription_table_init();
void subscription_table_destroy();
int remove_client(ClientTable *table, int client_fd);
int unsubscribe_client(ClientTable *table, int client_fd, const char *key);
void print_hash_table(ClientTable *table);
void subscribed_keys(const char *key,const char *value, int opcode);
void notify_client(int client_fd, const char *notif_pipe, const char *key, const char *value, int opcode);
int delete_key(ClientTable *table, const char *key);


#endif // KVS_H
