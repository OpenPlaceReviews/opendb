# opendb
This is a service that provides database communication

## Requirements
1. JDK minimum 8
2. Postgresql with empty database (default db: opengeoreviews, user: test, pwd: test)

Create database psql
```
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE openplacereviews OWNER test;
```

## How to build & run

```
cd java && ./gradlew bootRun
```

## Env variables
In order to be able to bootstrap first block with specified users. Note: You can always construct it yourself. You need to specify following env variables so the server will be able to sign messages
```
OPENDB_LOGIN=openplacereviews:test
OPENDB_PK=<SPECIFY>
OPENDB_PORT=6463
DB_URL=jdbc:postgresql://localhost:5432/openplacereviews
DB_USER=test
DB_PWD=test
```
