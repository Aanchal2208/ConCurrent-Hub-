# ConCurrent-Hub-

ConCurrent Hub

A multi-user collaborative data workspace built to handle concurrent updates efficiently.

ConCurrent Hub focuses on reducing database contention by handling record-level conflicts at the application layer. Requests working on different records can run in parallel, while requests targeting the same record are queued and processed sequentially.

The project is inspired by concepts from MxTasks and BareHeap.

#Features

- Multi-user collaborative workspace
- User authentication and role-based access
- Real-time updates using WebSockets
- Record-level task scheduling
- Conflict handling for simultaneous updates
- PostgreSQL transactions and concurrency control
- Application-level memory pooling
- Performance and load testing

#Tech Stack

- Frontend: React
- Backend: FastAPI, Python
- Database: PostgreSQL
- Real-time: WebSockets
- Deployment: Docker
- Testing: Locust, k6

#How It Works

User Requests
      |
      v
 FastAPI Backend
      |
      v
Task Scheduler
   /       \
No Conflict  Conflict
    |          |
 Parallel    Queue
    \          /
      v
  PostgreSQL

Non-conflicting operations are allowed to run together, while operations affecting the same record are handled one after another.

##OS & DBMS Concepts

#Operating Systems

- Concurrency
- Task scheduling
- Synchronization
- Memory management

#DBMS

- Transactions
- ACID properties
- MVCC
- Concurrency control
- Locking

#Performance Testing

The system will be tested under different workloads, including low contention, high contention, and burst traffic. Performance will be measured using throughput, CPU/memory usage, p95/p99 latency and database lock-wait time.

Team:

Team Nexus

Gargi Patra · Angel Odilo · Parth Gauniyal · Aanchal Singh

Project:

Academic project for Operating Systems (TCS-502) and DBMS (TCS-503).