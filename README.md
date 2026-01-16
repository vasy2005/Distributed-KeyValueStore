# Distributed Key-Value Store (KVS)

A high-performance, distributed key-value storage engine implemented in C. This project supports Master-Slave replication, asynchronous data persistence, and automatic memory-to-disk swapping (LRU).

## Features
* **Master-Slave Architecture**: Scale read operations by connecting multiple slaves to a master node.
* **Automatic Eviction (LRU)**: When RAM usage hits the 100MB limit, cold data is automatically moved to a binary swap file.
* **Data Persistence**: All write operations are logged asynchronously to an Append-Only File (AOF).
* **Atomic Transactions**: Support for `MULTI`, `EXECUTE`, and `DISCARD` commands with an internal undo-log for rollbacks.
* **TTL Support**: Keys can be set with an expiration time (Time-To-Live).

---

## 🛠️ Installation & Compilation

Ensure you have `gcc` and `make` (optional) installed on your Linux environment.

1.  **Clone the repository** and ensure all files (`server.c`, `client.c`, `thpool.c`, `thpool.h`, `hashmap.h`) are in the same directory.
2.  **Compile the Server**:
    ```bash
    gcc server.c thpool.c -o server -lpthread
    ```
3.  **Compile the Client**:
    ```bash
    gcc client.c -o client
    ```

---
