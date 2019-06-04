# opendb
This is a service that represents blockchain. It provides API to communicate with blockchain, such as:
- Query active objects
- Get active block chain
- Add operation to the queue
- Sign any message
- Generates login / signup keys
- Administrative operations: create block, revert block

More technical information is provided at https://github.com/OpenPlaceReviews/opendb/blob/master/java/opendb-api/README.md. 

# Requirements
1. JDK minimum 8
2. Postgresql with empty database (default db: opengeoreviews, user: test, pwd: test)
3. IPFS server optional

Create database psql
```
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE openplacereviews OWNER test;
```
Quick command to recreate schema from scratch
```
drop schema public cascade; create schema public; grant all on schema public to test;
```
# How to run IPFS
Start container
```
docker run -d --name ipfs_host -v $ipfs_staging:/export -v $ipfs_data:/data/ipfs -p 4001:4001 -p 127.0.0.1:8080:8080 -p 127.0.0.1:5001:5001 ipfs/go-ipfs:latest
```
Configure ipfs in application.yml
```
ipfs:
  run: ${IPFS_RUN:true}
  storing:
    time: ${IPFS_STORING_TIME:86400}
  host: ${IPFS_HOST:localhost} #dev host: 51.158.69.207
  port: ${IPFS_PORT:5001}
  timeout: ${IPFS_TIMEOUT:10000}
  directory: ${IPFS_DIRECTORY:/opendb/storage/}
  cluster:
    host: ${IPFS_CLUSTER_HOST:localhost}
    port: ${IPFS_CLUSTER_PORT:9094}
````

# How to build & run

```
cd java && ./gradlew bootRun
```
You should be able to see admin page at http://localhost:6463/api/admin.

# Env variables
In order to be able to bootstrap first block with specified users. Note: You can always construct it yourself. You need to specify following env variables so the server will be able to sign messages
```
## OPENDB_LOGIN=openplacereviews:test_1  # empty to use test_1 for development
## OPENDB_PK=  # empty to use test_1 for development network
OPENDB_PORT=6463
DB_URL=jdbc:postgresql://localhost:5432/openplacereviews
DB_USER=test
DB_PWD=test
```

Other variables are customizable could be found https://github.com/OpenPlaceReviews/opendb/blob/master/java/opendb-api/src/main/resources/application.yml
