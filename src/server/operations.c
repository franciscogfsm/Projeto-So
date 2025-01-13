#include "operations.h"
#include <pthread.h> 
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "src/common/constants.h"
#include "constants.h"
#include "io.h"
#include "kvs.h"
#include <stdbool.h>



static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER; // Protects the queue
static pthread_cond_t queue_not_empty = PTHREAD_COND_INITIALIZER;

ClientTable *subscription_table = NULL; 
// Create ClientTable
ClientTable *create_client_table() {
    ClientTable *table = malloc(sizeof(ClientTable));
    if (!table) {
        perror("Failed to allocate ClientTable");
        return NULL;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        table->table[i] = NULL;
    }
    pthread_rwlock_init(&table->lock, NULL);
    return table;
}

// Hash function
unsigned int hash_function(const char *key) {
    unsigned int hash = 0;
    while (*key) {
        hash = (hash * 31) + *key++;
    }
    return hash % TABLE_SIZE;
}

// Add a subscription
int add_subscription(ClientTable *table, const char *key, int client_fd, const char *notif_pipe) {
    unsigned int index = hash_function(key);

    pthread_rwlock_wrlock(&table->lock);

    SubscriptionNode *current = table->table[index];

    // Search for the key
    while (current) {
        if (strcmp(current->key, key) == 0) {
            // Key already exists, check if the client is already subscribed
            ClientNode *client_current = current->clients;
            while (client_current) {
                if (client_current->client_fd == client_fd) {
                    // Client is already subscribed, no need to add again
                    pthread_rwlock_unlock(&table->lock);
                    return 0;
                }
                client_current = client_current->next;
            }

            // Client is not subscribed, add the client
            ClientNode *new_client = malloc(sizeof(ClientNode));
            if (!new_client) {
                pthread_rwlock_unlock(&table->lock);
                return 1;
            }
            new_client->client_fd = client_fd;
            strncpy(new_client->notif_pipe, notif_pipe, MAX_STRING_SIZE);
            new_client->next = current->clients;
            current->clients = new_client;

            pthread_rwlock_unlock(&table->lock);
            return 0;
        }
        current = current->next;
    }

    // Key does not exist, create a new entry
    SubscriptionNode *new_key = malloc(sizeof(SubscriptionNode));
    if (!new_key) {
        pthread_rwlock_unlock(&table->lock);
        return 1;
    }
    strncpy(new_key->key, key, MAX_STRING_SIZE);
    new_key->clients = malloc(sizeof(ClientNode));
    if (!new_key->clients) {
        free(new_key);
        pthread_rwlock_unlock(&table->lock);
        return 1;
    }
    new_key->clients->client_fd = client_fd;
    strncpy(new_key->clients->notif_pipe, notif_pipe, MAX_STRING_SIZE);
    new_key->clients->next = NULL;

    new_key->next = table->table[index];
    table->table[index] = new_key;

    pthread_rwlock_unlock(&table->lock);
    return 0;
}
// Remove a subscription
int remove_subscription(ClientTable *table, const char *key, int client_fd) {
    unsigned int index = hash_function(key);

    pthread_rwlock_wrlock(&table->lock);

    SubscriptionNode *current = table->table[index];
    SubscriptionNode *prev_key = NULL;

    while (current) {
        if (strcmp(current->key, key) == 0) {
            // Encontrou a key
            ClientNode **indirect = &current->clients;
            while (*indirect) {
                if ((*indirect)->client_fd == client_fd) {
                    ClientNode *to_remove = *indirect;
                    *indirect = to_remove->next;
                    free(to_remove);
                    //INFO: Se for o ultimo cliente ele apaga a key por completo
                    if (!current->clients) {
                        if (prev_key) {
                            prev_key->next = current->next;
                        } else {
                            table->table[index] = current->next;
                        }
                        free(current);
                    }

                    pthread_rwlock_unlock(&table->lock);
                    return 0;
                }
                indirect = &(*indirect)->next;
            }
        }
        prev_key = current;
        current = current->next;
    }

    pthread_rwlock_unlock(&table->lock);
    return 1; // Key ou cliente não encontrado
}

void free_client_table(ClientTable *table) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        SubscriptionNode *current = table->table[i];
        while (current) {
            SubscriptionNode *key_to_free = current;
            current = current->next;

            ClientNode *client = key_to_free->clients;
            while (client) {
                ClientNode *client_to_free = client;
                client = client->next;
                free(client_to_free);
            }

            free(key_to_free);
        }
    }
    pthread_rwlock_destroy(&table->lock);
    free(table);
}

// Initialize subscription table
int subscription_table_init() {
    if (subscription_table != NULL) {
        fprintf(stderr, "Subscription table already initialized.\n");
        return 1;
    }

    subscription_table = create_client_table();
    if (!subscription_table) {
        fprintf(stderr, "Failed to initialize subscription table.\n");
        return 1;
    }
    return 0;
}

// Destroy subscription table
void subscription_table_destroy() {
    if (subscription_table != NULL) {
        free_client_table(subscription_table);
        subscription_table = NULL;
    }
}

int remove_client(ClientTable *table, int client_fd) {
    if (!table) {
        fprintf(stderr, "Invalid table\n");
        return 1;
    }
    pthread_rwlock_wrlock(&table->lock);
    for (int i = 0; i < TABLE_SIZE; i++) {
        SubscriptionNode *current = table->table[i];
        SubscriptionNode *prev_key = NULL;
        while (current) {
            ClientNode **indirect = &current->clients;
            while (*indirect) {
                if ((*indirect)->client_fd == client_fd) {
                    ClientNode *to_remove = *indirect;
                    *indirect = to_remove->next;
                    free(to_remove);
                    // If no more clients, remove the key
                    if (!current->clients) {
                        if (prev_key) {
                            prev_key->next = current->next;
                        } else {
                            table->table[i] = current->next;
                        }
                        free(current);
                        break; // Move to the next key
                    }
                } else {
                    indirect = &(*indirect)->next;
                }
            }
            prev_key = current;
            current = current->next;
        }
    }
    pthread_rwlock_unlock(&table->lock);
    return 0;
}


static struct HashTable *kvs_table = NULL;
// Subscribe client
int subscribe_client(ClientTable *table, int client_fd, const char *key, const char *notif_pipe) {
    if (!key_exists(kvs_table, key)){
      return 1;
    }
    if (!table || !key || !notif_pipe) {
        fprintf(stderr, "Invalid table, key, or notification pipe\n");
        return 1;
    }

    if (add_subscription(table, key, client_fd, notif_pipe) != 0) {
        fprintf(stderr, "Failed to subscribe client_fd %d to key %s\n", client_fd, key);
        return 1;
    }
    return 0;
}




int unsubscribe_client(ClientTable *table, int client_fd, const char *key) {
    if (!key_exists(kvs_table, key)){
      return 1;
    }
    if (!table || !key) {
        fprintf(stderr, "Invalid table or key\n");
        return 1;
    }

    if (remove_subscription(table, key, client_fd) != 0) {
        fprintf(stderr, "Failed to unsubscribe client_fd %d to key %s\n", client_fd, key);
        return 1;
    }
    return 0;
}



void print_hash_table(ClientTable *table) {
    if (!table) {
        printf("Hash table is NULL\n");
        return;
    }

    pthread_rwlock_rdlock(&table->lock);

    for (int i = 0; i < TABLE_SIZE; i++) {
        SubscriptionNode *current = table->table[i];
        if (current) {
            printf("Index %d:\n", i);
        }
        while (current) {
            printf("  Key: %s\n", current->key);
            ClientNode *client = current->clients;
            while (client) {
                printf("    Client FD: %d\n", client->client_fd);
                client = client->next;
            }
            current = current->next;
        }
    }

    pthread_rwlock_unlock(&table->lock);
}
int delete_key(ClientTable *table, const char *key) {
    if (!table || !key) {
        fprintf(stderr, "Invalid table or key\n");
        return 1;
    }

    unsigned int index = hash_function(key);

    pthread_rwlock_wrlock(&table->lock);

    SubscriptionNode *current = table->table[index];
    SubscriptionNode *prev = NULL;

    while (current) {
        if (strcmp(current->key, key) == 0) {
            // Found the key, remove it
            if (prev) {
                prev->next = current->next;
            } else {
                table->table[index] = current->next;
            }

            // Free the client list
            ClientNode *client = current->clients;
            while (client) {
                ClientNode *to_free = client;
                client = client->next;
                free(to_free);
            }

            // Free the subscription node
            free(current);

            pthread_rwlock_unlock(&table->lock);
            return 0;
        }
        prev = current;
        current = current->next;
    }
    pthread_rwlock_unlock(&table->lock);
    return 1;
}




void subscribed_keys(const char *key,const char *value, int opcode) {
    int found=0;
    if (!subscription_table || !key) {
        fprintf(stderr, "Invalid table or key\n");
        return;
    }
    
    
    unsigned int index = hash_function(key);

    pthread_rwlock_rdlock(&subscription_table->lock);

    SubscriptionNode *current = subscription_table->table[index];
    while (current) {
        if (strcmp(current->key, key) == 0) {
            found=1;
            ClientNode *client = current->clients;
            while (client) {
                notify_client(client->client_fd, client->notif_pipe, key,value,opcode);
                client = client->next;
            }
            break; // Key found
        }
        current = current->next;
    }
    pthread_rwlock_unlock(&subscription_table->lock);
    if (opcode == 6 && found != 0){
      delete_key(subscription_table,key);
    }
}



void notify_client(int client_fd, const char *notif_pipe, const char *key, const char *value, int opcode) {
    struct {
        int opcode;
        char key[MAX_STRING_SIZE];
        char value[MAX_STRING_SIZE];
    } message;

    message.opcode = opcode;
    strncpy(message.key, key, MAX_STRING_SIZE);
    if (opcode!=6){
      strncpy(message.value, value, MAX_STRING_SIZE);
    }
    


    int notif_fd = open(notif_pipe, O_WRONLY);
    if (notif_fd == -1) {
        perror("Failed to open notification pipe");
        return;
    }


    if (write(notif_fd, &message, sizeof(message)) == -1) {
        perror("Failed to write to notification pipe");
    }

    close(notif_fd);
}












/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);


  for (size_t i = 0; i < num_pairs; i++) {
    
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write key pair (%s,%s)\n", keys[i], values[i]);
      continue;
    }
    //TODO: if new key to write == old key then dont notify and continue do that function 
    subscribed_keys(keys[i],values[i],5);
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
      //O pois o delete nao precisa de moistrar value
    }else{
      subscribed_keys(keys[i],NULL,6);
    }
  }
  if (aux) {
    write_str(fd, "]\n");
  }
  
  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  char aux[MAX_STRING_SIZE];

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key,
               keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
}

int kvs_backup(size_t num_backup, char *job_filename, char *directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory,
           strtok(job_filename, "."), num_backup);

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  pid = fork();
  pthread_rwlock_unlock(&kvs_table->tablelock);
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->key,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ", ",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->value,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ")\n",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}










/// Queue-related functions.
int queue_init(Queue *queue, size_t capacity) {
    queue->nodes = malloc(capacity * sizeof(QueueNode));
    if (queue->nodes == NULL) {
        return 1;
    }
    queue->capacity = capacity;
    queue->size = 0;
    queue->front = 0;
    queue->rear = 0;
    return 0;
}
int queue_enqueue(Queue *queue, int client_fd) {
    pthread_mutex_lock(&queue_mutex);

    if (queue->size == queue->capacity) {
        pthread_mutex_unlock(&queue_mutex);
        return 1; 
    }

    queue->nodes[queue->rear].client_fd = client_fd;
    queue->rear = (queue->rear + 1) % queue->capacity;
    queue->size++;

    pthread_cond_signal(&queue_not_empty); 
    pthread_mutex_unlock(&queue_mutex);
    return 0;
}
int queue_dequeue(Queue *queue, int *client_fd) {
    pthread_mutex_lock(&queue_mutex);

    while (queue->size == 0) {
        pthread_cond_wait(&queue_not_empty, &queue_mutex); // Wait for clients
    }

    *client_fd = queue->nodes[queue->front].client_fd;
    queue->front = (queue->front + 1) % queue->capacity;
    queue->size--;

    pthread_mutex_unlock(&queue_mutex);
    return 0;
}

int queue_is_empty(Queue *queue) {
    pthread_mutex_lock(&queue_mutex);
    int is_empty = (queue->size == 0);
    pthread_mutex_unlock(&queue_mutex);
    return is_empty;
}

int queue_is_full(Queue *queue) {
    pthread_mutex_lock(&queue_mutex);
    int is_full = (queue->size == queue->capacity);
    pthread_mutex_unlock(&queue_mutex);
    return is_full;
}

void queue_destroy(Queue *queue) {
    free(queue->nodes);
    queue->nodes = NULL;
    queue->capacity = 0;
    queue->size = 0;
    queue->front = 0;
    queue->rear = 0;
}
