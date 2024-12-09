#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include "constants.h"
#include "parser.h"
#include <fcntl.h>
#include "operations.h"
#include <string.h>

///home/kali/Desktop/Projeto-So/tests-public/jobs/


int main() {
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }
    
    char buffer[MAX_WRITE_SIZE]; // Buffer for the directory name
    ssize_t bytesReadD;
    DIR *dir;
    struct dirent *entry;

    
    bytesReadD = read(STDIN_FILENO, buffer, sizeof(buffer) - 1);
    if (bytesReadD == -1) {
        fprintf(stderr, "Failed to read from stdin Input\n");
        return 1;
    }

    buffer[bytesReadD] = '\0';
    buffer[strcspn(buffer, "\n")] = '\0';

    dir = opendir(buffer);
    if (dir == NULL) {
        perror("opendir");
        return 1;
    }

    while ((entry = readdir(dir)) != NULL) {
        char *p_dot = strrchr(entry->d_name, '.');

        if (p_dot && strcmp(p_dot, ".job") == 0) {
            char file_path[512];
            snprintf(file_path, sizeof(file_path), "%s/%s", buffer, entry->d_name);
            int fd = open(file_path, O_RDONLY);
            if (fd == -1) {
                fprintf(stderr, "Failed to open File\n");
                continue;
            }
            
            
            char out_filename[256];
            strncpy(out_filename, entry->d_name, sizeof(out_filename));
            char *dot = strrchr(out_filename, '.');
            if (dot != NULL) {
                *dot = '\0';
            }
            char final_out_filename[512]; // Ensure the buffer is large enough to hold the filename and ".out" extension
            snprintf(final_out_filename, sizeof(final_out_filename), "%s.out", out_filename);

            int fd_out = open(out_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd_out == -1) {
                fprintf(stderr, "Failed to Create File\n");
                continue;
            }
            

                while(1){
                    enum Command cmd = get_next(fd);
                    if (cmd == EOC) {
                        break;
                    }
                    //printf("Command retrieved: %d\n", cmd);
                    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                    unsigned int delay;
                    size_t num_pairs;
                    switch (cmd) {
                        case CMD_WRITE:
                            num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                            if (num_pairs == 0) {
                                continue;
                            }
                            if (kvs_write(num_pairs, keys, values)) {
                                fprintf(stderr, "Failed to write pair\n");
                            }
                            break;

                        case CMD_READ:
                            num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                            if (num_pairs == 0) {
                                fprintf(stderr, "Invalid command. See HELP for usage\n");
                                continue;
                            }
                            if (kvs_read(num_pairs,keys,fd_out)) {
                                fprintf(stderr, "Failed to read pair\n");
                            }
                            break;

                        case CMD_DELETE:

                            num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                            if (num_pairs == 0) {
                                fprintf(stderr, "Invalid command. See HELP for usage\n");
                                continue;
                            }
                            if (kvs_delete(num_pairs, keys,fd_out)) {
                                fprintf(stderr, "Failed to delete pair\n");
                            }
                            break;

                        case CMD_SHOW:
                            kvs_show(fd_out);
                            break;

                        case CMD_WAIT:
                            if (parse_wait(fd, &delay, NULL) == -1) {
                                fprintf(stderr, "Invalid command. See HELP for usage\n");
                                continue;
                            }
                            if (delay > 0) {
                                printf("Waiting...\n");
                                kvs_wait(delay);
                            }
                            break;

                        case CMD_BACKUP:
                            if (kvs_backup()) {
                                fprintf(stderr, "Failed to perform backup.\n");
                            }
                            break;

                        case CMD_INVALID:
                            fprintf(stderr, "Invalid command. See HELP for usage\n");
                            break;

                        case CMD_HELP:
                            printf(
                                "Available commands:\n"
                                "  WRITE [(key,value)(key2,value2),...]\n"
                                "  READ [key,key2,...]\n"
                                "  DELETE [key,key2,...]\n"
                                "  SHOW\n"
                                "  WAIT <delay_ms>\n"
                                "  BACKUP\n" // Not implemented
                                "  HELP\n"
                            );
                            break;

                        case CMD_EMPTY:
                            break;

                        case EOC:
                            kvs_terminate();
                            exit(0);
                            fprintf(stderr, "Invalid command. See HELP for usage\n");
                   }
                }

            close(fd);
        }
    }
    // Close the directory
    closedir(dir);
    printf("Finished without problems\n");
    return 0;
}
