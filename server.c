#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>

#include "thpool.h"
#include "hashmap.h"

//TODO SLAVE: la reconexiune, isi pierde baza de date

#define MASTER 0
#define SLAVE 1
#define MSG_LEN 1000 //TODO: Could be buffer overflow
#define MAX_SLAVES 10
#define QUEUE_LEN 100
#define FD_MAX 1024
#define READ_WORKERS 8
#define MAX_RAM 300LL
#define EVICTION_LIMIT 0.9f
#define EVICTION_BATCH_SIZE 20

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
    int is_client; //slaves are also treated as clients
    int active;
    int is_multi; //for MULTI
    char queue[QUEUE_LEN][MSG_LEN]; int st, dr;//for MULTI
} Client;

int master_compute(int);
int slave_compute(int);
void delete_slave(int);
int get(int, const char*, char*);
HashEntry* set(const char*, const char*, long long);
int del(const char*);
int fd_write(int, const char*);
int fd_read(int, char*);
int slave_setup(char*, int);
const char* get_ip_from_fd(int);
int execute_multi_commands(int, char [][MSG_LEN], int, int);
int atomic_get(int, const char*, char*);
void strip_msg(char*);
void get_from_disk(void *arg);
void* eviction(void *arg);
int write_to_swap(char*, int);
void send_to_evict();
void check_expired();
int load_save_file();

int PORT = 5000;
int server_type = MASTER;

// Variables for MASTER
fd_set slavefds;
SlaveNode slaves[MAX_SLAVES];
int slave_counter = 0;

// Variables for SLAVE
int fd_master;

// Variables for both
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

int eviction_active = 0;
int current_check_expired = 0;

int swap_fd;

FILE* save_fd;

typedef struct 
{
    pthread_t idThread;
    int thCount;
} Thread;

threadpool disk_thpool; //for reading from disk
int disk_pipe[2];
typedef struct 
{
    long long offset;
    long long len;
    char* key;
    int client_fd;
    int pipe_fd;
} DiskJob;

typedef struct
{
    char* value;
    char* key;
    int client_fd;
} DiskJobResult;

Thread eviction_thread;
int main_to_thread_evictpipe[2];
int thread_to_main_evictpipe[2];
typedef struct
{
    long long offset;
    char key[MSG_LEN];
    int version;
} EvictionResult;
typedef struct
{
    char *value;
    char key[MSG_LEN];
    long long len;
    int version;
} EvictionMsg;

typedef enum
{
    NONE,
    RESTORE_RAM,
    RESTORE_DISK
} UndoType;
typedef struct
{
    char key[MSG_LEN];
    UndoType type;

    char* ram_value;
    int value_len;

    long long disk_offset;

    long long expiry_time;
    int version;
} UndoLog;
void create_undo_entry(UndoLog*, const char*);

int global_file_end = 0;

Client clients[FD_MAX];

int main(int argc, char *argv[])
{
    //initial setup
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

    char swap_file[MSG_LEN];
    sprintf(swap_file, "swap_file_%d.bin", PORT);
    if ((swap_fd = open(swap_file, O_RDWR | O_CREAT | O_TRUNC, 0666)) == -1)
    {
        perror("Swap file couldn't be created / opened");
        exit(1);
    }

    char save_file[MSG_LEN];
    sprintf(save_file, "save_file_%d.txt", PORT);
    if ((save_fd = fopen(save_file, "a+")) == NULL)
        perror("Save file doesn't exists \\ couldn't be created");
    

    if (pipe(disk_pipe) < 0)
    {
        perror("Pipe couldn't be created.");
        exit(1);
    }

    if (create_hashmap() == NULL)
    {
        printf("Hashmap couldn't be created.");
        exit(1);
    }

    disk_thpool = thpool_init(READ_WORKERS);

    pthread_create(&eviction_thread.idThread, NULL, eviction, NULL);
    if (pipe(main_to_thread_evictpipe) < 0)
    {
        perror("Pipe couldn't be created.");
        exit(1);
    }
    if (pipe(thread_to_main_evictpipe) < 0)
    {
        perror("Pipe couldn't be created.");
        exit(1);
    }

    lru_init();

    //server connection
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
    FD_SET(disk_pipe[0], &actfds);
    FD_SET(thread_to_main_evictpipe[0], &actfds);

    tv.tv_sec = 1;
    tv.tv_usec = 0;

    nfds = sd;

    if (load_save_file() < 0)
        printf("Error while loading save file\n");
    printf("[server] Asteptam la portul %d...\n", PORT);
    fflush(stdout);
    
    //server starting
    FD_ZERO(&slavefds);
    while(1)
    {
        memcpy((char*)&readfds, (char*)&actfds, sizeof(readfds));
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        //TODO: use epoll
        if (select(nfds+1, &readfds, NULL, NULL, &tv) < 0)
        {
            perror("[server] Eroare la select().\n");
            return errno;
        }

        if (FD_ISSET(thread_to_main_evictpipe[0], &readfds))
        {
            FD_CLR(thread_to_main_evictpipe[0], &readfds);
            EvictionResult result;
            read(thread_to_main_evictpipe[0], &result, sizeof(EvictionResult));
            HashEntry* entry = get_from_key(result.key);
            
            if (entry != NULL && result.version == entry->version)
            {
                free(entry->storage.ram_value);

                entry->is_swapped = 1;
                entry->storage.disk_offset = result.offset;
                
                lru_remove(entry);
                
                entry->pending_eviction = 0;
                
                memory_used -= entry->value_len+1;
                
                printf("[server] Resursa cu cheia %s evacuata pe swap file\n", entry->key);
            }
        }

        if (FD_ISSET(disk_pipe[0], &readfds))
        {
            FD_CLR(disk_pipe[0], &readfds);
            DiskJobResult result;
            read(disk_pipe[0], &result, sizeof(DiskJobResult));
            HashEntry* entry = get_from_key(result.key);

            if (entry != NULL)
            {
                if (entry->is_swapped == 1)
                {
                    entry->storage.ram_value = result.value;
                    entry->is_swapped = 0;
                    memory_used += entry->value_len + 1;

                    printf("[server] Resursa cu cheia %s luata de pe swap file.", entry->key);
                }
                else
                    free(result.value);
                lru_promote(entry);
                
                fd_write(result.client_fd, entry->storage.ram_value);

                FD_SET(result.client_fd, &actfds); 
            }
            else
                {
                    free(result.value);
                    fd_write(result.client_fd, "GET FAILED");
                }
            free(result.key);
        }

        if (FD_ISSET(sd, &readfds))
        {
            FD_CLR(sd, &readfds);
            len = sizeof(from);
            memset(&from, 0, sizeof(from));

            client = accept(sd, (struct sockaddr*)&from, &len);

            clients[client].active = 1;
            clients[client].is_multi = 0;
            clients[client].st = 0; clients[client].dr = -1;
            clients[client].is_client = 1;

            if (client < 0)
            {
                perror("[server] Eroare la accept().\n");
                continue;
            }

            if (nfds < client)
                nfds = client;

            FD_SET(client, &actfds);

            printf("[server] S-a conectat client/slave cu descriptorul %d, de la adresa %s.\n", client, conv_addr(from));
            fflush(stdout);
        }

        for (fd = 0; fd <= nfds; fd++)
            if (FD_ISSET(fd, &readfds))
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
            
        send_to_evict();
        check_expired();
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
            clients[fd].is_multi = 1;
        if (ans[0] != 0 && fd_write(fd, ans) < 0)
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
                if (execute_multi_commands(fd, clients[fd].queue, clients[fd].st, clients[fd].dr) < 0)
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
        clients[fd].is_client = 0;
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
            ans[0] = 0;
            char value[MSG_LEN];
            int status = 0;
            if ((status = get(fd, key, value)) < 0)
                strcpy(ans, "[SERVER] GET FAILED");
            else
                if (status > 0)
                    strcpy(ans, value);
                else
                    return bytes;
                
            if (ans[0] != 0 && fd_write(fd, ans) < 0)
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

    char value[MSG_LEN]; long long ttl;
    if (sscanf(msg, "SET %s %s %lld", key, value, &ttl) == 3)
    {
        if (set(key, value, ttl) == NULL)
            strcpy(ans, "[SERVER] SET FAILED");
        else
        {
            for (int i = 0; i < slave_counter; i++)
                fd_write(slaves[i].fd, msg);
            
            char notif[MSG_LEN]; notif[0] = 0;
            sprintf(notif, "Key %s set to Value %s with TTL %lld by client with file descriptor %d", key, value, ttl, fd);
            notify_clients(notif);
        }

        if (ans[0] > 0 && fd_write(fd, ans) < 0)
            return -1;
        return bytes;
    }

    if (sscanf(msg, "DEL %s", key) == 1)
    {
        if (del(key) < 0)
            strcpy(ans, "[SERVER] DEL FAILED");
        else
            {
                for (int i = 0; i < slave_counter; i++)
                    fd_write(slaves[i].fd, msg);
                char notif[MSG_LEN]; notif[0] = 0;
                sprintf(notif, "Key %s deleted by client with file descriptor %d", key, fd);
                notify_clients(notif); 
            }
        if (ans[0] > 0 && fd_write(fd, ans) < 0)
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
        ans[0] = 0;
        char value[MSG_LEN];
        int status = 0;
        if ((status = get(fd, key, value)) < 0)
            strcpy(ans, "[SERVER] GET FAILED");
        else
            if (status > 0)
                strcpy(ans, value);
            else
                return bytes;

        if (ans[0] != 0 && fd_write(fd, ans) <= 0)
            return -1;
        return bytes;
    }



    char value[MSG_LEN]; long long ttl;
    if (sscanf(msg, "SET %s %s %lld", key, value, &ttl) == 3)
    {
        if (set(key, value, ttl) == NULL)
            strcpy(ans, "[server] SET FAILED.\n");

        printf("%s\n", ans); fflush(stdout);
        return bytes;
    }

    if (sscanf(msg, "DEL %s", key) == 1)
    {
        if (del(key) < 0)
            strcpy(ans, "[SERVER] DEL FAILED");

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

int get(int fd, const char* key, char* value)
{
    value[0] = 0;
    HashEntry* entry = get_from_key(key);
    if (entry == NULL)
        return -1;

    if (entry->is_swapped == 0)
    {
        lru_promote(entry);
        strcpy(value, entry->storage.ram_value);
        return 1;
    }

    //add get to thread, block fd
    //add to thread
    DiskJob* job = malloc(sizeof(DiskJob));
    job->offset = entry->storage.disk_offset;
    job->client_fd = fd;
    job->len = entry->value_len;
    job->pipe_fd = disk_pipe[1];
    job->key = strdup(entry->key);

    thpool_add_work(disk_thpool, get_from_disk, job);
    FD_CLR(fd, &actfds);
    return 0;
}

void get_from_disk(void *arg)
{
    DiskJob* job = (DiskJob*)arg;

    char* buffer = malloc(job->len+1);

    int bytes = pread(swap_fd, buffer, job->len, job->offset);
    
    if (bytes > 0)
        buffer[bytes] = 0;
    else
        buffer[0] = 0;

    DiskJobResult result;

    result.client_fd = job->client_fd;
    result.key = job->key;
    result.value = buffer;

    write(job->pipe_fd, &result, sizeof(result));

    free(job);
}

HashEntry* set(const char* key, const char* value, long long ttl)
{
    HashEntry* entry = insert_in_hash(key, value, ttl);
    
    if (entry != NULL)
    {
        lru_promote(entry);
        fprintf(save_fd, "SET %s %s %lld\n", key, value, ttl); fflush(save_fd);
    }
    return entry;
}

int del(const char* key)
{
    if (delete_from_hash(key) < 0)
        return -1;
    fprintf(save_fd, "DEL %s\n", key); fflush(save_fd);
    return 0; //-1 for error
}

int execute_multi_commands(int fd, char queue[][MSG_LEN], int st, int dr)
{
    UndoLog logs[100]; int index = 0;
    char msgs[100][MSG_LEN]; int no_msg = 0;
    
    int success = 1;
    for (int i = st; i <= dr; i++)
    {
        success = 1;
        char* msg = queue[st++];
        char ans[MSG_LEN]; ans[0] = 0;
        char key[MSG_LEN];
        if (sscanf(msg, "GET %s", key) == 1)
        {
            ans[0] = 0;
            char value[MSG_LEN];
            int status = 0;
            if ((status = atomic_get(fd, key, value)) < 0)
            {
                strcpy(ans, "[SERVER] GET FAILED");
                success = 0;
            }
            else
                strcpy(ans, value);
                
            if (ans[0] != 0 && fd_write(fd, ans) < 0)
                success = 0;
            if (success == 0)
                break;
            else
                continue;
        }

        char value[MSG_LEN]; long long ttl;
        if (sscanf(msg, "SET %s %s %lld", key, value, &ttl) == 3)
        {   
            strcpy(msgs[no_msg++], msg);
            create_undo_entry(&logs[index++], key);

            if (set(key, value, ttl) == NULL)
            {
                strcpy(ans, "[SERVER] SET FAILED");
                success = 0;
            }

            if (ans[0] > 0 && fd_write(fd, ans) < 0)
                success = 0;
            if (success == 0)
                break;
            else
                continue;
        }

        if (sscanf(msg, "DEL %s", key) == 1)
        {
            strcpy(msgs[no_msg++], msg);
            create_undo_entry(&logs[index++], key);
            if (del(key) < 0)
            {
                success = 0;
                strcpy(ans, "[SERVER] DEL FAILED");
            }
            else
                strcpy(ans, "[SERVER] DEL DONE SUCCESSFULLY");

            if (fd_write(fd, ans) < 0)
                success = 0;
            if (success == 0)
                break;
            else
                continue;
        }

        strcpy(ans, "[SERVER] UNKNOWN COMMAND");
        fd_write(fd, ans);
        success = 0;
        break;
    }

    if (success == 0)
    {
        for (int i = index - 1; i >= 0; i--)
        {
            if (logs[i].type == NONE)
            {
                del(logs[i].key);
                continue;
            }

            HashEntry* entry;
            if (logs[i].type == RESTORE_RAM)
                entry = set(logs[i].key, logs[i].ram_value, logs[i].expiry_time);
            else
                {
                    entry = set(logs[i].key, "a", logs[i].expiry_time);
                    free(entry->storage.ram_value); memory_used -= 2;
                    entry->storage.disk_offset = logs[i].disk_offset;
                    entry->is_swapped = 1;
                }
            
            entry->version = logs[i].version;
            entry->value_len = logs[i].value_len;
        }    

        for (int i = index - 1; i >= 0; i--)
            if (logs[i].type == RESTORE_RAM && logs[i].ram_value != NULL)
                free(logs[i].ram_value);
        return -1;
    }

    fprintf(save_fd, "MULTI\n");

    for (int i = index - 1; i >= 0; i--)
        if (logs[i].type == RESTORE_RAM && logs[i].ram_value != NULL)
            free(logs[i].ram_value);

    for (int i = 0; i < no_msg; i++)
    {
        for (int j = 0; j < slave_counter; j++)
            if (strncmp(msgs[i], "GET", 3) != 0)
                fd_write(slaves[j].fd, msgs[i]);
    }
    return 0;
}

void create_undo_entry(UndoLog* log, const char* key)
{
    HashEntry* entry = get_from_key(key);

    if (entry == NULL)
    {
        log->type = NONE;
        strcpy(log->key, key);
        return;
    }

    log->value_len = entry->value_len;
    log->version = entry->version;
    log->expiry_time = entry->expiry_time - time(NULL);

    strcpy(log->key, entry->key);

    if (entry->is_swapped == 0)
    {
        log->type = RESTORE_RAM;
        log->ram_value = strdup(entry->storage.ram_value);
    }
    else
        {
            log->type=RESTORE_DISK;
            log->disk_offset = entry->storage.disk_offset;
        }
}

int atomic_get(int fd, const char* key, char* value)
{
    value[0] = 0;
    HashEntry* entry = get_from_key(key);
    if (entry == NULL)
        return -1;

    if (entry->is_swapped == 0)
    {
        lru_promote(entry);
        strcpy(value, entry->storage.ram_value);
    }
    else
        {
            char* buffer = malloc(entry->value_len+1);
            if (buffer == NULL)
                return -1;

            int bytes = pread(swap_fd, buffer, entry->value_len, entry->storage.disk_offset);
            
            if (bytes < 0)
                return -1;

            if (bytes > 0)
                buffer[bytes] = 0;
            else
                buffer[0] = 0;

            entry->storage.ram_value = buffer;
            entry->is_swapped = 0;
            memory_used += entry->value_len + 1;

            printf("[server] Resursa cu cheia %s luata de pe swap file.\n", entry->key);

            lru_promote(entry);
        }

    return 0;
}

void send_to_evict()
{
    double usage_ratio = memory_used*1.0 / MAX_RAM;

    if (usage_ratio > EVICTION_LIMIT)
        eviction_active = 1;

    if (eviction_active == 1 && usage_ratio <= EVICTION_LIMIT - 0.2)
        eviction_active = 0;
        
    if (eviction_active == 1)
    {
        // printf("Memory Used: %d, Ratio: %f\n", memory_used, usage_ratio); fflush(stdout);
        HashEntry* entry = lru_cache.tail;
        for (int i = 0; entry != NULL && i < EVICTION_BATCH_SIZE; i++, 
                                entry = entry->lru_prev)
        {
            if (entry->pending_eviction == 1)
            {
                i--;
                continue;
            }
            char* value = strdup(entry->storage.ram_value);
            EvictionMsg msg;
            msg.len = entry->value_len;
            msg.value = value;
            msg.key[0] = 0;
            msg.version = entry->version;
            strcpy(msg.key, entry->key);
            write(main_to_thread_evictpipe[1], &msg, sizeof(EvictionMsg));
            entry->pending_eviction = 1;
        }
    }
}

void* eviction(void* arg)
{
    EvictionMsg msg;
    EvictionResult result;
    while(1)
    {
        if (read(main_to_thread_evictpipe[0], &msg, sizeof(EvictionMsg)) >= 0)
        {
            result.offset = write_to_swap(msg.value, msg.len);
            result.key[0] = 0; strcpy(result.key, msg.key);
            result.version = msg.version;

            free(msg.value);

            write(thread_to_main_evictpipe[1], &result, sizeof(EvictionResult));
        }
    }
}

int write_to_swap(char* value, int len)
{
    long long offset = global_file_end;
    global_file_end += len;

    pwrite(swap_fd, value, len, offset);

    return offset;
}

void check_expired()
{
    time_t now = time(NULL);
    for (int i = 0; i < 20; i++)
    {
        current_check_expired = (current_check_expired + 1) % TABLE_SIZE;

        HashEntry* it = hashmap[current_check_expired];
        while (it != NULL)
        {
            HashEntry* next = it->nextEntry;

            if (now > it->expiry_time)
            {
                printf("Elementul cu key %s a expirat\n", it->key); fflush(stdout);
                
                if (server_type == MASTER)
                {
                    char notif[MSG_LEN]; notif[0] = 0;
                    sprintf(notif, "Key %s has expired and has been deleted.", it->key);
                    notify_clients(notif);
                }
                del(it->key);
            }
            it = next;
        }
    }
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

    if (msg_len > MSG_LEN-1)
        msg_len = MSG_LEN-1;
    
    bytes = read(fd, msg, msg_len);
    if (bytes < 0)
        perror("[server] Eroare la read() mesaj de la client.\n");
    else
        msg[bytes] = 0;

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

void notify_clients(const char* msg)
{
    for (int i = 0; i < FD_MAX; i++)
        if (clients[i].active == 1 && clients[i].is_client == 1)
            fd_write(i, msg);
}

int load_save_file()
{
    char msg[MSG_LEN];
    while (fgets(msg, MSG_LEN, save_fd) != NULL)
    {
        strip_msg(msg);

        char key[MSG_LEN], value[MSG_LEN]; long long ttl;
        if (sscanf(msg, "SET %s %s %lld", key, value, &ttl) == 3)
        {
            HashEntry* entry = insert_in_hash(key, value, ttl);
    
            if (entry != NULL)
                lru_promote(entry);
            else
                return -1;
            continue;
        }

        if (sscanf(msg, "DEL %s", key) == 1)
            delete_from_hash(key);
    }

    printf("Finished loading saved database.\n"); fflush(stdout);
    return 0;
}