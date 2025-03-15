# CC-Memcached - A Lightweight Memcached Server Implementation

CC-Memcached is a custom implementation of a **Memcached-compatible server**, designed as part of the **[Memcached Challenge](https://codingchallenges.fyi/challenges/challenge-memcached)**. It supports **basic caching operations** like `SET`, `GET`, `ADD`, `REPLACE`, `APPEND`, and `PREPEND`, along with expiration times.

## **Features**

- **Memcached Text Protocol Support** (Compatible with `memtier_benchmark`)
- **Multi-threaded Support** (Handles concurrent clients efficiently)
- **Expiration Handling** (Supports TTL for cache entries)
- **High Performance** (Tested with `memtier_benchmark` under load)
- **Efficient Synchronization** using `sync.Map` for thread safety

---

## **Installation**

### **Prerequisites**

- **Go 1.18+** (Download from [golang.org](https://golang.org/dl/))
- **memtier_benchmark** (For performance testing)  
  Install using:
  ```sh
  git clone https://github.com/RedisLabs/memtier_benchmark
  cd memtier_benchmark
  autoreconf -ivf && ./configure && make && sudo make install
  ```

---

## **Usage**

### **1. Clone the repository:**

```sh
git clone https://github.com/nullsploit01/cc-memcached.git
cd cc-memcached
```

### **2. Build the project:**

```sh
go build -o cc-memcached
```

### **3. Run the server:**

```sh
./cc-memcached
```

---

## **Benchmarking with `memtier_benchmark`**

To test the performance of CC-Memcached, use the following command:

```sh
memtier_benchmark -p 11211 -P memcache_text \
  -n 100000 -c 20 -t 5 --ratio=1:1 \
  --key-pattern=S:S --data-size=128
```

### **Benchmark Results:**

```
ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets       107874.61          ---          ---         0.18622         0.17500         0.42300         1.07900     17358.73
Gets       107874.61    107874.61         0.00         0.18472         0.17500         0.41500         1.04700     19020.86
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals     215749.22    107874.61         0.00         0.18547         0.17500         0.42300         1.06300     36379.60
```

**100% Cache Hit Rate**
**Ultra-low Latency (~0.18ms per request)**
**Over 215,000 Operations Per Second**

---

## **License**

This project is open-source and available under the **MIT License**.

---

## **🔗 Links**

- **Challenge URL:** [Memcached Challenge](https://codingchallenges.fyi/challenges/challenge-memcached)
- **Benchmark Tool:** [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark)
