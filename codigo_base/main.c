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
    //Find the paths for all of the job files and put it in the array
    int num_jobs;
    char **Job_paths = find_jobs(dir, buffer, &num_jobs);
    if (Job_paths == NULL) {
        return 1;
    }
    char file_out[512];
    for(int j = 0; j < num_jobs; j++){
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;
        

        int fd = open(Job_paths[j], O_RDONLY);
        if (fd == -1) {
            fprintf(stderr, "Failed to open File\n");
            continue;
        }
        char *file_name = get_file_name(Job_paths[j]);
        
        snprintf(file_out, sizeof(file_out), "%s.out", file_name);
        free(file_name);
        int fd_out = open(file_out, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out == -1) {
            fprintf(stderr, "Failed to Create File\n");
            continue;
        }
            while(1){
                enum Command cmd = get_next(fd);
                if (cmd == EOC) {
                    break;
                }
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
                        break;close(fd_out);

                    case CMD_EMPTY:
                        break;

                    case EOC:
                        kvs_terminate();
                        exit(0);
                        fprintf(stderr, "Invalid command. See HELP for usage\n");
                }
            }
        close(fd);
        close(fd_out);
    }
    closedir(dir);
    printf("Finished without problems\n");
    free_job_paths(Job_paths, num_jobs);
    return 0;
}
