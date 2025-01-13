#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

typedef struct {
    char notif_pipe_path[256];
    int notif_fd;
} NotificationPIPE;



//INFO: OPCODE CHAVE,OUTRACHAVE
void *notification_handler(void *arg) {
    NotificationPIPE *notif_args = (NotificationPIPE *)arg;
    int notif_fd = open(notif_args->notif_pipe_path, O_RDONLY);
    if (notif_fd == -1) {
        perror("Failed to open notification pipe");
        free(notif_args);
        return NULL;
    }

    struct {
        int opcode;
        char key[MAX_STRING_SIZE];
        char value[MAX_STRING_SIZE];
    } message;

    while (1) {
        ssize_t bytes_read = read(notif_fd, &message, sizeof(message));
        if (bytes_read > 0) {
            switch (message.opcode) {
                case 5:
                    printf("(%s,%s)\n", message.key, message.value);
                    break;
                case 6:
                    printf("(%s,DELETED)\n", message.key);
                    break;
                default:
                    fprintf(stderr, "Unknown opcode: %d\n", message.opcode);
                    break;
            }
        } else if (bytes_read == 0) {
            break;
        } else {
            perror("Error reading from notification pipe");
            break;
        }
    }
    close(notif_fd);
    free(notif_args);
    return NULL;
}





int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));


  if(kvs_connect(req_pipe_path, resp_pipe_path, argv[2], notif_pipe_path) != 0){
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }
  // Abrir o pipe de notificações
  int notif_fd = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);
  if (notif_fd == -1) {
      fprintf(stderr, "Failed to open notification pipe");
      kvs_disconnect();
      return 1;
  }

  NotificationPIPE *notif_args = malloc(sizeof(NotificationPIPE));

  if (!notif_args) {
      fprintf(stderr,"Failed to allocate memory for notification arguments");
      close(notif_fd);
      kvs_disconnect();
      return 1;
  }
  strncpy(notif_args->notif_pipe_path, notif_pipe_path, 
  sizeof(notif_args->notif_pipe_path));
  notif_args->notif_fd = notif_fd;

  // Criar a thread de notificações
  pthread_t notif_thread;
  if (pthread_create(&notif_thread, NULL, notification_handler, notif_args) != 0) {
      fprintf(stderr, "Failed to create notification thread\n");
      free(notif_args);
      close(notif_fd);
      kvs_disconnect();
      return 1;
  }
  
  

  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if (kvs_disconnect() != 0) {
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }
      
      printf("Disconnected from server\n");
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(keys[0])) {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(keys[0])) {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0) {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      // input should end in a disconnect, or it will loop here forever
      break;
    }
  }
}
