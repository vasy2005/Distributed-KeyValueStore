#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>

#define MASTER 0
#define SLAVE 1
#define MSG_LEN 100
#define MAX_SLAVES 10
#define QUEUE_LEN 100
#define FD_MAX 1024

extern int errno;

char * conv_addr (struct sockaddr_in address)
{
  static char str[25];
  char port[7];

  /* adresa IP a clientului */
  strcpy (str, inet_ntoa (address.sin_addr));	
  /* portul utilizat de client */
  memset(port, 0, 7);
  sprintf (port, ":%d", ntohs (address.sin_port));	
  strcat (str, port);
  return (str);
}

typedef struct 
{
    int fd;
    int client_counter;
    char ip[16];
    int port;
} SlaveNode;

typedef struct 
{
    int active;
    int is_multi; //for MULTI
    char queue[QUEUE_LEN][MSG_LEN]; int st, dr;//for MULTI
} Client;

int master_compute(int);
int slave_compute(int);
void delete_slave(int);
int get(const char*, char*);
int set(const char*, const char*, int);
int del(const char*);
int fd_write(int, const char*);
int fd_read(int, char*);
int slave_setup(char*, int);
const char* get_ip_from_fd(int);
int execute_multi_commands(char [][MSG_LEN], int, int);
void strip_msg(char*);

int PORT = 5000;
int server_type = MASTER;

// Variables for MASTER
fd_set slavefds;
SlaveNode slaves[MAX_SLAVES];
int slave_counter = 0;

// Variables for SLAVE
int fd_master;

Client clients[FD_MAX];

int main(int argc, char *argv[])
{
    PORT = atoi(argv[1]);
    if (argc == 5 && !strcmp(argv[2], "SLAVE"))
        server_type = SLAVE;

    if (server_type == MASTER)
        printf("Incepem serverul MASTER...\n");
    else
        printf("Incepem serverul SLAVE...\n");
    fflush(stdout);

    if (server_type == SLAVE)
    {
        printf("Incercam conexiune cu MASTER...\n");
        fflush(stdout);
        fd_master = slave_setup(argv[3], atoi(argv[4]));
    }

    struct sockaddr_in server;
    struct sockaddr_in from;
    fd_set readfds;
    fd_set actfds;
    struct timeval tv;
    int sd, client;
    int optval = 1;
    int fd;

    int nfds;
    int len;

    if ((sd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror("[server] Eroare la socket().\n");
        return errno;
    }

    setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    memset(&server, 0, sizeof(server));

    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(PORT);

    if (bind(sd, (struct sockaddr*)&server, sizeof(struct sockaddr)) == -1)
    {
        perror("[server] Eroare la bind().\n");
        return errno;
    }

    if (listen(sd, 5) == -1)
    {
        perror("[server] Eroare la listen().\n");
        return errno;
    }

    FD_ZERO(&actfds);
    FD_SET(sd, &actfds);
    if (server_type == SLAVE)
        FD_SET(fd_master, &actfds);

    tv.tv_sec = 1;
    tv.tv_usec = 0;

    nfds = sd;

    printf("[server] Asteptam la portul %d...\n", PORT);
    fflush(stdout);
    
    FD_ZERO(&slavefds);
    while(1)
    {
        memcpy((char*)&readfds, (char*)&actfds, sizeof(readfds));
        
        if (select(nfds+1, &readfds, NULL, NULL, &tv) < 0)
        {
            perror("[server] Eroare la select().\n");
            return errno;
        }

        //TODO: la fiecare secunda testez daca trebuie sters cnv, pe MASTER

        if (FD_ISSET(sd, &readfds))
        {
            len = sizeof(from);
            memset(&from, 0, sizeof(from));

            client = accept(sd, (struct sockaddr*)&from, &len);

            clients[client].active = 1;
            clients[client].is_multi = 0;
            clients[client].st = 0; clients[client].dr = -1;

            if (client < 0)
            {
                perror("[server] Eroare la accept().\n");
                continue;
            }

            if (nfds < client)
                nfds = client;

            FD_SET(client, &actfds);

            printf("[server] S-a conectat clientul cu descriptorul %d, de la adresa %s.\n", client, conv_addr(from));
            fflush(stdout);
        }

        for (fd = 0; fd <= nfds; fd++)
            if (fd != sd && FD_ISSET(fd, &readfds))
            {
                int(*compute)(int) = 0;
                if (server_type == MASTER)
                    compute = master_compute;
                else
                    compute = slave_compute;

                if (compute(fd) <= 0)
                {
                    close(fd);
                    FD_CLR(fd, &actfds);

                    if (FD_ISSET(fd, &slavefds))
                    {
                        FD_CLR(fd, &slavefds); close(fd);
                        delete_slave(fd);
                        printf("[server] S-a deconectat slave-ul cu descriptorul %d.\n", fd);
                    }
                    else
                        printf("[server] S-a deconectat clientul cu descriptorul %d.\n", fd);

                }

            }       
    }
}

int slave_setup(char *ip, int port)
{
    struct sockaddr_in server;
    int sd;
    if ((sd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror ("[client] Eroare la socket() la conectare cu master.\n");
        exit(errno);
    }

    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(ip);
    server.sin_port = htons(port);

    if (connect(sd, (struct sockaddr*)&server, sizeof(struct sockaddr)) == -1)
    {
        perror ("[client]Eroare la connect() la conectare cu master.\n");
        exit(errno);
    }

    char msg[MSG_LEN];
    int slave_port = PORT;
    sprintf(msg, "SLAVE %d", slave_port);
    fd_write(sd, msg);
    return sd;
}

int master_compute(int fd)
{
    char msg[MSG_LEN]; memset(msg, 0, sizeof(msg));
    char ans[MSG_LEN]; memset(ans, 0, sizeof(ans));
    int msg_len = 0; 
    int bytes;

    bytes = fd_read(fd, msg); 
    if (bytes <= 0)
        return bytes;
    strip_msg(msg);
    
    printf ("[server] Mesajul a fost receptionat...%s\n", msg);

    if (strcmp(msg, "MULTI") == 0)
    {
        if (clients[fd].is_multi == 1)
            strcpy(ans, "[SERVER] MULTI MODE IS ALREADY SET");
        else
            clients[fd].is_multi = 1, strcpy(ans, "[SERVER] MULTI MODE SET");
        if (fd_write(fd, ans) < 0)
            return -1;
        return bytes;
    }

    if (strcmp(msg, "EXECUTE") == 0)
    {
        if (clients[fd].is_multi == 0)
            strcpy(ans, "MULTI MODE NOT ACTIVATED, THERE IS NOTHING TO EXECUTE");
        else
            {
                clients[fd].is_multi = 0;
                if (execute_multi_commands(clients[fd].queue, clients[fd].st, clients[fd].dr) < 0)
                    strcpy(ans, "[SERVER] ERROR, COMMANDS DISCARDED");
                else
                    strcpy(ans, "[SERVER] COMMANDS EXECUTED SUCCESSFULLY");
                clients[fd].st = 0; clients[fd].dr = -1;
            }
        
        if (fd_write(fd, ans) < 0)
            return -1;
        return bytes;
    }

    if (strcmp(msg, "DISCARD") == 0)
    {
        if (clients[fd].is_multi == 0)
            strcpy(ans, "[SERVER] MULTI MODE NOT ACTIVATED, THERE IS NOTHING TO DISCARD");
        else
            {
                clients[fd].is_multi = 0;
                clients[fd].st = 0;
                clients[fd].dr = -1;
                strcpy(ans, "[SERVER] COMMANDS DISCARDED");
            }
        
        if (fd_write(fd, ans) < 0)
            return -1;
        return bytes;
    }

    if (clients[fd].is_multi == 1)
    {
        clients[fd].dr++;
        strcpy(clients[fd].queue[clients[fd].dr], msg);
        return bytes;
    }

    if (sscanf(msg, "SLAVE %d", &slaves[slave_counter].port) == 1)
    {
        slaves[slave_counter].fd = fd;
        slaves[slave_counter].client_counter = 0;
        const char *ip = get_ip_from_fd(fd);
        if (ip != NULL)
            strcpy(slaves[slave_counter].ip, ip);
        else
            printf ("[server] Nu am putut lua ip-ul SLAVE-ului cu descriptor %d\n", fd);

        slave_counter ++;

        FD_SET(fd, &slavefds);
        printf ("[server] Slave cu descriptor %d s-a conectat\n", fd);
        return bytes;
    }

    char key[MSG_LEN];
    if (sscanf(msg, "GET %s", key) == 1)
    {
        if (slave_counter == 0)
        {
            char value[MSG_LEN];
            if (get(key, value) < 0)
                strcpy(ans, "[SERVER] GET FAILED");
            else
                strcpy(ans, value);
                
            if (fd_write(fd, ans) < 0)
                return -1;
            return bytes;
        }

        int minim = 1000000000, mini = -1;
        for (int i = 0; i < slave_counter; i++)
            if (minim > slaves[i].client_counter)
            {
                minim = slaves[i].client_counter;
                mini = i;
            }

        sprintf(ans, "CONN %s %d GET %s", slaves[mini].ip, slaves[mini].port, key);
        slaves[mini].client_counter ++;
        
        if (fd_write(fd, ans) < 0)
            return -1;
        return bytes;
    }

    char value[MSG_LEN]; int ttl;
    if (sscanf(msg, "SET %s %s %d", key, value, &ttl) == 3)
    {
        if (set(key, value, ttl) < 0)
            strcpy(ans, "[SERVER] SET FAILED");
        else
            strcpy(ans, "[SERVER] SET DONE SUCCESSFULLY");

        for (int i = 0; i < slave_counter; i++)
            fd_write(slaves[i].fd, msg);

        if (fd_write(fd, ans) < 0)
            return -1;
        return bytes;
    }

    if (sscanf(msg, "DEL %s", key) == 1)
    {
        if (del(key) < 0)
            strcpy(ans, "[SERVER] DEL FAILED");
        else
            strcpy(ans, "[SERVER] DEL DONE SUCCESSFULLY");

        for (int i = 0; i < slave_counter; i++)
            fd_write(slaves[i].fd, msg);

        if (fd_write(fd, ans) < 0)
            return -1;
        return bytes;
    }

    strcpy(ans, "[SERVER] UNKNOWN COMMAND");
    if (fd_write(fd, ans) <= 0)
        return -1;

    return bytes;
}

int slave_compute(int fd)
{
    char msg[MSG_LEN]; memset(msg, 0, sizeof(msg));
    char ans[MSG_LEN]; memset(ans, 0, sizeof(ans));
    int msg_len = 0; 
    int bytes;

    bytes = fd_read(fd, msg);
    if (bytes <= 0)
        return bytes;

    printf ("[server] Mesajul a fost receptionat...%s\n", msg);

    char key[MSG_LEN];
    if (sscanf(msg, "GET %s", key) == 1)
    {
        char value[MSG_LEN];
        if (get(key, value) < 0)
            strcpy(ans, "[SERVER] GET FAILED");
        else
            strcpy(ans, value);
            
        if (fd_write(fd, ans) <= 0)
            return -1;
        return bytes;
    }

    char value[MSG_LEN]; int ttl;
    if (sscanf(msg, "SET %s %s %d", key, value, &ttl) == 3)
    {
        if (set(key, value, ttl) < 0)
            strcpy(ans, "[server] SET FAILED.\n");
        else
            strcpy(ans, "[server] SET DONE SUCCESSFULLY.\n");

        printf("%s\n", ans); fflush(stdout);
        return bytes;
    }

    if (sscanf(msg, "DEL %s", key) == 1)
    {
        if (del(key) < 0)
            strcpy(ans, "[SERVER] DEL FAILED");
        else
            strcpy(ans, "[SERVER] DEL DONE SUCCESSFULLY");

        printf("%s\n", ans); fflush(stdout);
        return bytes;
    }

    if (fd != fd_master)
        strcpy(ans, "[SERVER] UNKNOWN COMMAND");
    if (fd_write(fd, ans) <= 0)
        return -1;
    return bytes;
}

void delete_slave(int fd)
{
    int pos = -1;
    for (int i = 0; i < slave_counter; i++)
        if (slaves[i].fd == fd)
        {
            pos = i;
            break;
        }

    for (int i = pos; i < slave_counter-1; i++)
        memcpy(&slaves[i], &slaves[i+1], sizeof(SlaveNode));
    slave_counter --;
}

int get(const char* key, char* value)
{
    //get value from memory
    strcpy(value, key);
    return 0; //-1 for error
}

int set(const char* key, const char* value, int ttl)
{
    //add key:value
    return 0; //-1 for error
}

int del(const char* key)
{
    //add key:value
    return 0; //-1 for error
}

int execute_multi_commands(char queue[][MSG_LEN], int st, int dr)
{
    //execute multi commands

    return 0; //-1 for faiilure
}

int fd_write(int fd, const char *msg)
{
    int len = strlen(msg);
    int bytes = write(fd, &len, 4);
    if (bytes <= 0)
    {
        perror ("[server] Eroare la write() lungime catre client.\n");
        return -1;
    }

    bytes = write(fd, msg, strlen(msg));
    if (bytes <= 0)
    {
        perror ("[server] Eroare la write() mesaj catre client.\n");
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
        perror("[server] Eroare la read() lungime mesaj de la client.\n");
        return bytes;
    }
    
    bytes = read(fd, msg, msg_len);
    if (bytes < 0)
        perror("[server] Eroare la read() mesaj de la client.\n");

    return bytes;
}

const char* get_ip_from_fd(int fd)
{
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);

    if (getpeername(fd, (struct sockaddr*) &addr, &len) == 0)
        return inet_ntoa(addr.sin_addr);
    
    return NULL;
}

void strip_msg(char* msg)
{
    int i;
    for (i = 0; msg[i] != 0 && msg[i] != '\n'; i++);    
    msg[i] = 0;
}