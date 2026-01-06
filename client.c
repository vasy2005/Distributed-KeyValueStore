#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#define MSG_LEN 1000
#define STDIN 0

#define COL_RESET   "\x1b[0m"
#define COL_RED     "\x1b[31m"     
#define COL_GREEN   "\x1b[32m"    
#define COL_YELLOW  "\x1b[33m"     
#define COL_BLUE    "\x1b[34m"    
#define COL_CYAN    "\x1b[36m"    
#define COL_MAGENTA "\x1b[35m"     

extern int errno;

int port;

int master_fd;

int slave_fd = -1;
char ip_slave[16]; int port_slave;
int is_multi = 0;

char prompt[MSG_LEN];

int fd_write(int, const char*);
int fd_read(int, char*);
int connect_to_server(const char*, int);
void strip_msg(char*);
void print_colored_message(char*);

int main(int argc, char *argv[])
{
    char msg[MSG_LEN];

    if (argc != 3)
    {
        printf ("[client] Sintaxa: %s <adresa_server> <port>\n", argv[0]);
        return -1;
    }

    master_fd = connect_to_server(argv[1], atoi(argv[2]));

    fd_set readfds;
    fd_set actfds;
    struct timeval tv;
    int nfds;

    FD_ZERO(&actfds);
    FD_SET(master_fd, &actfds);
    FD_SET(STDIN, &actfds);
    
    tv.tv_sec = 1000; tv.tv_usec = 0; //TODO: add client timeout
    nfds = master_fd;
    
    sprintf(prompt, "%skv-store> %s", COL_CYAN, COL_RESET);
    printf("%s=== Client Pornit. Scrie comenzi (GET, SET, DEL...) ===%s\n", COL_GREEN, COL_RESET);

    printf("%s", prompt); fflush(stdout);
    memset(msg, 0, sizeof(msg));
    while(1)
    {
        memcpy(&readfds, &actfds, sizeof(readfds));
        memset(msg, 0, MSG_LEN);

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        if (select(nfds+1, &readfds, NULL, NULL, &tv) < 0)
        {
            perror ("[client] Eroare la select().\n");
            return errno;
        }
        
        //TODO: is master is closed, exit()
        
        if (slave_fd >= 0 && FD_ISSET(slave_fd, &readfds))
        {
            if (fd_read(slave_fd, msg) <= 0)
            {
                print_colored_message("[client] Slave a inchis conexiunea.\n");
                close(slave_fd); FD_CLR(slave_fd, &actfds);
                slave_fd = -1;
            }
            else
                print_colored_message(msg);
            continue;
        }

        if (FD_ISSET(master_fd, &readfds))
        {
            if (fd_read(master_fd, msg) <= 0)
            {
                printf("\n%s[client] Master a inchis conexiunea.%s\n", COL_RED, COL_RESET); fflush(stdout);
                close(master_fd);
                exit(0);
            }

            char ip[20]; int port;
            char comm[MSG_LEN];
            if (sscanf(msg, "CONN %s %d %[^\n]", ip, &port, comm) == 3)
            {
                char mesaj[MSG_LEN]; mesaj[0] = 0;
                sprintf(mesaj, "[client] Primit CONN catre Slave %s:%d...\n", ip, port);
                print_colored_message(mesaj);
                slave_fd = connect_to_server(ip, port);
                FD_SET(slave_fd, &actfds);
                if (nfds < slave_fd)
                    nfds = slave_fd;
                    
                strcpy(msg, comm);
                if (fd_write(slave_fd, msg) < 0)
                {
                    perror ("[client] Eroare la write() spre server.\n");
                    return errno;
                }
                continue;
            }

            print_colored_message(msg);
            continue;
        }


        if (FD_ISSET(STDIN, &readfds))
        {
            int bytes = read(0, msg, MSG_LEN);
            strip_msg(msg);

            for (int i = 0; msg[i] != ' ' && msg[i] != 0; i++)
                msg[i] = toupper(msg[i]);

            if (msg != NULL && msg[0] != 0)
            {
                int crt_fd;
                if (slave_fd >= 0)
                    crt_fd = slave_fd;
                else
                    crt_fd = master_fd;

                // Always send these to master
                if (strncmp(msg, "SET", 3) == 0 || strncmp(msg, "DEL", 3) == 0)
                    crt_fd = master_fd;
                if (strcmp(msg, "MULTI") == 0)
                    is_multi = 1;

                if (is_multi == 1)
                    crt_fd = master_fd;

                if (strcmp(msg, "DISCARD") == 0 || strcmp(msg, "EXECUTE") == 0)
                    is_multi = 0;

                if (fd_write(crt_fd, msg) < 0)
                {
                    perror ("[client] Eroare la write() spre server.\n");
                    return errno;
                }
            }

            printf("%s", prompt); fflush(stdout);
        }

        
    }

    close(master_fd);
}

void print_colored_message(char* msg)
{
    printf("\r\x1b[K");

    if (strstr(msg, "[FAILED]") || strstr(msg, "[ERROR]"))
        printf("%s %s%s", COL_RED, msg, COL_RESET);
    else
    if (strstr(msg, "[SUCCESS]") || strstr(msg, "[DONE]"))
        printf("%s %s%s", COL_GREEN, msg, COL_RESET);
    else
    if (strstr(msg, "[NOTIFY]"))
        printf("%s %s%s", COL_YELLOW, msg, COL_RESET);
    else
        printf("%s %s%s \n", COL_BLUE, msg, COL_RESET);

    printf("%s", prompt);
    fflush(stdout);
}

void strip_msg(char* msg)
{
    int i;
    for (i = 0; msg[i] != 0 && msg[i] != '\n'; i++);    
    msg[i] = 0;
}

int connect_to_server(const char* ip, int port)
{
    int sd;
    struct sockaddr_in server;
    if ((sd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror ("[client] Eroare la socket().\n");
        return errno;
    }

    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(ip);    
    server.sin_port = htons(port);

    if (connect(sd, (struct sockaddr*)&server, sizeof(struct sockaddr)) == -1)
    {
        perror("[client] Eroare la connect().\n");
        exit(errno);
    }
    return sd;
}

int fd_write(int fd, const char *msg)
{
    int len = strlen(msg);
    int bytes = write(fd, &len, 4);
    if (bytes <= 0)
    {
        perror ("[client] Eroare la write() lungime catre client.\n");
        return -1;
    }

    bytes = write(fd, msg, strlen(msg));
    if (bytes <= 0)
    {
        perror ("[client] Eroare la write() mesaj catre client.\n");
        return -1;
    }

    return bytes;
}

int fd_read(int fd, char* msg)
{
    int bytes, msg_len;
    bytes = read(fd, &msg_len, 4);
    if (bytes < 0)
    {
        perror("[client] Eroare la read() lungime mesaj de la client.\n");
        return bytes;
    }
    
    bytes = read(fd, msg, msg_len);
    if (bytes < 0)
        perror("[client] Eroare la read() mesaj de la client.\n");

    return bytes;
}