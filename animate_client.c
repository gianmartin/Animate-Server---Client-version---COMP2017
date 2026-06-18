#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <ctype.h>


volatile int is_logged_in = 0;
char login_username[64] = {0};

void trim_whitespace(char *str) {
    char *end;
    while(isspace((unsigned char)*str)) str++;
    if(*str == 0) return;
    end = str + strlen(str) - 1;
    while(end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
}

// Background thread to read responses asynchronously
void* response_reader(void* arg) {
    int fd_s2c = *(int*)arg;
    FILE* stream = fdopen(fd_s2c, "r");
    char buffer[256];

    while (fgets(buffer, sizeof(buffer), stream)) {
        trim_whitespace(buffer);
        
        if (strncmp(buffer, "Reject", 6) == 0) {
            printf("%s\n", buffer);
            fflush(stdout);
            is_logged_in = 0;
            exit(1);
        } 
        else if (!is_logged_in) {
            // If not reject and not logged in, must be the balance
            printf("Welcome %s. Your balance is %s\n", login_username,
                buffer);
            fflush(stdout);
            is_logged_in = 1;
        } 
        else {
            // General RPC response for other calls
            printf("SERVER: %s\n", buffer);
        }
    }
    
    printf("Server disconnected.\n");
    exit(0);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <server pid>\n", argv[0]);
        exit(1);
    }

    pid_t server_pid = atoi(argv[1]);
    pid_t my_pid = getpid();


    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR2);
    sigprocmask(SIG_BLOCK, &mask, NULL); 

    // Send SIGUSR1 to server
    if (kill(server_pid, SIGUSR1) == -1) {
        perror("Failed to send signal to server");
        exit(1);
    }

    // Block until SIGUSR2 or 1 second timeout
    struct timespec timeout = {1, 0}; 
    int sig = sigtimedwait(&mask, NULL, &timeout);
    
    if (sig < 0) {
        fprintf(stderr, "Timeout or error waiting for server response.\n");
        exit(1);
    }

    // Open FIFOs
    char fifo_c2s[128], fifo_s2c[128];
    snprintf(fifo_c2s, sizeof(fifo_c2s), "FIFO_C2S_%d", my_pid);
    snprintf(fifo_s2c, sizeof(fifo_s2c), "FIFO_S2C_%d", my_pid);

    int fd_c2s = open(fifo_c2s, O_WRONLY);
    int fd_s2c = open(fifo_s2c, O_RDONLY);

    if (fd_c2s < 0 || fd_s2c < 0) {
        perror("Failed to open FIFOs");
        exit(1);
    }

    // Start background reader thread
    pthread_t reader_tid;
    pthread_create(&reader_tid, NULL, response_reader, &fd_s2c);
    pthread_detach(reader_tid);

    // Main loop waiting for user input
    char input[256];
    while (fgets(input, sizeof(input), stdin)) {
        trim_whitespace(input);
        if (strlen(input) == 0) continue;

        if (!is_logged_in) {
            char parsed_user[64];
            if (sscanf(input, "Login %63s", parsed_user) == 1) {
                // Save the username to print in the welcome message later
                strncpy(login_username, parsed_user, sizeof(login_username)
                    - 1);
                
                // Send exactly as specified
                char formatted_cmd[256];
                snprintf(formatted_cmd, sizeof(formatted_cmd), "%s\n", input);
                write(fd_c2s, formatted_cmd, strlen(formatted_cmd));
            } else {
                printf("Not logged in\n");
            }
        } else {
            // Already logged in, send standard RPC commands
            char formatted_cmd[256];
            snprintf(formatted_cmd, sizeof(formatted_cmd), "%s\n", input);
            write(fd_c2s, formatted_cmd, strlen(formatted_cmd));
        }
    }

    close(fd_c2s);
    return 0;
}