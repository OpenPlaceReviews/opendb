# opendb
This is a service that represents blockchain. It provides API to communicate with blockchain, such as:
- Query active objects
- Get active block chain
- Add operation to the queue
- Sign any message
- Generates login / signup keys
- Administrative operations: create block, revert block

## Requirements
1. JDK minimum 8
2. Postgresql with empty database (default db: opengeoreviews, user: test, pwd: test)

Create database psql
```
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE openplacereviews OWNER test;
```
Quick command to recreate schema from scratch
```
drop schema public cascade; create schema public; grant all on schema public to test;
```

## How to build & run

```
cd java && ./gradlew bootRun
```
You should be able to see admin page at http://localhost:6463/api/admin.

## Env variables
In order to be able to bootstrap first block with specified users. Note: You can always construct it yourself. You need to specify following env variables so the server will be able to sign messages
```
# OPENDB_LOGIN=openplacereviews:test_1  # empty to use test_1 for development
# OPENDB_PK=  # empty to use test_1 for development network
OPENDB_PORT=6463
DB_URL=jdbc:postgresql://localhost:5432/openplacereviews
DB_USER=test
DB_PWD=test
```
