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

#define MSG_LEN 1000
#define STDIN 0

extern int errno;

int port;

int master_fd;

int slave_fd = -1;
char ip_slave[16]; int port_slave;
int is_multi = 0;

int fd_write(int, const char*);
int fd_read(int, char*);
int connect_to_server(const char*, int);
void strip_msg(char* msg);

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
    
    memset(msg, 0, sizeof(msg));
    while(1)
    {
        
        memcpy(&readfds, &actfds, sizeof(readfds));
        memset(msg, 0, MSG_LEN);
        
        if (select(nfds+1, &readfds, NULL, NULL, NULL) < 0)
        {
            perror ("[client] Eroare la select().\n");
            return errno;
        }
        
        //TODO: is master is closed, exit()
        
        if (slave_fd >= 0 && FD_ISSET(slave_fd, &readfds))
        {
            if (fd_read(slave_fd, msg) <= 0)
            {
                printf("[client] Slave a inchis conexiunea.\n"); fflush(stdout);
                close(slave_fd); FD_CLR(slave_fd, &actfds);
                slave_fd = -1;
            }
            else
            printf("%s\n", msg), fflush(stdout);
            continue;
        }

        if (FD_ISSET(master_fd, &readfds))
        {
            if (fd_read(master_fd, msg) <= 0)
            {
                printf("[client] Master a inchis conexiunea.\n"); fflush(stdout);
                close(master_fd);
                exit(0);
            }

            char ip[20]; int port;
            char comm[MSG_LEN];
            if (sscanf(msg, "CONN %s %d %[^\n]", ip, &port, comm) == 3)
            {
                printf("[client] Primit CONN catre Slave %s:%d...\n", ip, port);

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

            printf("%s\n", msg); fflush(stdout);
            continue;
        }

        if (FD_ISSET(STDIN, &readfds))
        {
            int bytes = read(0, msg, MSG_LEN);
            strip_msg(msg);

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
    }

    close(master_fd);
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