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


void process_line(const char *line, int fd, off_t line_start_pos) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    enum Command command = get_next(line);
    printf("Processing line: %s\n", line);
    switch (command) {
        case CMD_WRITE:
            // Reset the file descriptor to the beginning of the line
            lseek(fd, (off_t)((unsigned long)line_start_pos + strlen("WRITE ")), SEEK_SET);

            
            num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            if (num_pairs == 0) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                return;
            }
            if (kvs_write(num_pairs, keys, values)) {
                fprintf(stderr, "Failed to write pair\n");
            }
            break;

        case CMD_READ:
            // Reset the file descriptor to the beginning of the line
            lseek(fd, (off_t)((unsigned long)line_start_pos + strlen("READ ")), SEEK_SET);
            num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            if (num_pairs == 0) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                return;
            }
            if (kvs_read(num_pairs, keys)) {
                fprintf(stderr, "Failed to read pair\n");
            }
            break;

        case CMD_DELETE:
            // Reset the file descriptor to the beginning of the line
            lseek(fd, (off_t)((unsigned long)line_start_pos + strlen("DELETE ")), SEEK_SET);
            num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            if (num_pairs == 0) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                return;
            }
            if (kvs_delete(num_pairs, keys)) {
                fprintf(stderr, "Failed to delete pair\n");
            }
            break;

        case CMD_SHOW:
            kvs_show();
            break;

        case CMD_WAIT:
            // Reset the file descriptor to the beginning of the line
            lseek(fd, (off_t)((unsigned long)line_start_pos + strlen("WAIT ")), SEEK_SET);
            if (parse_wait(fd, &delay, NULL) == -1) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                return;
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
    }
}

int main() {
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    char buffer[MAX_WRITE_SIZE]; // Buffer for the directory name
    ssize_t bytesReadD;
    DIR *dir;
    struct dirent *entry;

    printf(">");
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
            char file_path[600];
            snprintf(file_path, sizeof(file_path), "%s/%s", buffer, entry->d_name);
            int fd = open(file_path, O_RDONLY);
            if (fd == -1) {
                perror("open");
                continue;
            }

            char line[100];
            ssize_t bytesRead;
            size_t line_length = 0;
            off_t line_start_pos = lseek(fd, 0, SEEK_CUR);

            while ((bytesRead = read(fd, line + line_length, sizeof(line) - line_length - 1)) > 0) {
                line_length += (size_t)bytesRead;
                line[line_length] = '\0';

                char *line_start = line;
                char *line_end;
                while ((line_end = strchr(line_start, '\n')) != NULL) {
                    *line_end = '\0';
                    process_line(line_start, fd, line_start_pos);
                    line_start = line_end + 1;
                    line_start_pos = lseek(fd, 0, SEEK_CUR) - (line_end - line_start);
                }

                // Move the remaining part of the buffer to the beginning
                line_length = strlen(line_start);
                memmove(line, line_start, line_length);
            }

            // Process any remaining line if the file does not end with a newline
            if (line_length > 0) {
                line[line_length] = '\0';
                process_line(line, fd, line_start_pos);
            }

            // Close the file
            close(fd);
        }
    }

    // Close the directory
    closedir(dir);
    printf("Finished without problems\n");
    return 0;
}
