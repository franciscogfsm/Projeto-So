#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "kvs.h"
#include "constants.h"
#include <fcntl.h>
#include "parser.h"
#include <stdbool.h>

static struct HashTable* kvs_table = NULL;


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
  return 0;
}
int compare_keys(const void *a, const void *b) {
    const char *key_a = (const char *)a;
    const char *key_b = (const char *)b;
    return strcmp(key_a, key_b);
}


void global_line_locker() {
    for (size_t i = 0; i < TABLE_SIZE; i++) {
        pthread_rwlock_rdlock(&kvs_table->locks[i]); // Acquire read lock
    }
}

void global_line_unlocker() {
  for (size_t i = 0; i < TABLE_SIZE; i++) {
    pthread_rwlock_unlock(&kvs_table->locks[i]); // Release lock
  }
}

void line_locker(size_t num_pairs, char keys[][MAX_STRING_SIZE], int write) {
    qsort(keys, num_pairs, sizeof(keys[0]), compare_keys); 
    bool locked[TABLE_SIZE] = {false}; 
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        if (!locked[index]) { 
            if (write) {
                pthread_rwlock_wrlock(&kvs_table->locks[index]);
            } else {
                pthread_rwlock_rdlock(&kvs_table->locks[index]);
            }
            locked[index] = true; 
        }
    }
}

void line_unlocker(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
    qsort(keys, num_pairs, sizeof(keys[0]), compare_keys); 
    bool unlocked[TABLE_SIZE] = {false}; 

    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        if (!unlocked[index]) { 
            pthread_rwlock_unlock(&kvs_table->locks[index]);
            unlocked[index] = true; 
        }
    }
}



int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }
  return 0;
}



int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  qsort(keys, num_pairs, sizeof(keys[0]), compare_keys);
  
  write(fd_out, "[", 1);
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    char buffer[256];
    if (result == NULL) {
      snprintf(buffer, sizeof(buffer), "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(buffer, sizeof(buffer), "(%s,%s)", keys[i], result);
    }
    write(fd_out, buffer, strlen(buffer));
    free(result);
  }
  write(fd_out, "]\n", 2);
  return 0;
}


int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  char buffer[MAX_WRITE_SIZE];
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write(fd_out, "[", 1);
        aux = 1;
      }
      snprintf(buffer, sizeof(buffer), "(%s,KVSMISSING)", keys[i]);
      write(fd_out, buffer, strlen(buffer));
    }
  }
  if (aux) {
    write(fd_out, "]\n", 2);
  }
  return 0;
}

void kvs_show(int fd_out) {
  char buffer[256];
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key, keyNode->value);
      ssize_t bytes_written = write(fd_out, buffer, strlen(buffer));
      if (bytes_written == -1) {
        perror("write");
        close(fd_out);
        return;
      }
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

void format_Char(char *backup_filename, size_t size, const char *full_path, const char *buffer, int file_bcks) {
    char *input_file_name = get_file_name(full_path);
    
    // Remove the .job extension if it exists
    char file_name_no_ext[256];
    strncpy(file_name_no_ext, input_file_name, sizeof(file_name_no_ext));
    char *dot = strrchr(file_name_no_ext, '.');
    if (dot && !strcmp(dot, ".job")) {
        *dot = '\0';
    }

    snprintf(backup_filename, size, "%s/%s-%d.bck", buffer, file_name_no_ext, file_bcks);
    free(input_file_name);
}

int kvs_backup(const char *full_path, const char *buffer, int file_bcks) {
    char backup_filename[256];
    format_Char(backup_filename, sizeof(backup_filename), full_path, buffer, file_bcks);
    int fd = open(backup_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("open");
        return -1;
    }
    pid_t pid = fork();
    if (pid == 0) {
        // Child process
        global_line_locker();
        for (int i = 0; i < TABLE_SIZE; i++) {
            KeyNode *keyNode = kvs_table->table[i];
            while (keyNode != NULL) {
                const char *key = keyNode->key;
                const char *value = keyNode->value;
                const char *prefix = "(";
                const char *separator = ", ";
                const char *suffix = ")\n";

                // Write key
                write(fd, prefix, 1);
                write(fd, key, strlen(key));
                write(fd, separator, 2);
                write(fd, value, strlen(value));
                write(fd, suffix, 2);

                keyNode = keyNode->next;
            }
        }
        global_line_unlocker();
        close(fd);
        _exit(0);
    } else if (pid > 0) {
        // Parent process
        close(fd);
        return 0;
    } else {
        // Fork failed
        perror("fork");
        close(fd);
        unlink(backup_filename);
        return -1;
    }
}


void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}







