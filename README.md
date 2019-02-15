# opendb
This is a service that provides database communication

## Requirements
1. JDK minimum 8
2. Postgresql with empty database (default db: opengeoreviews, user: test, pwd: test)

## How to build & run
```
cd java && ./gradlew bootRun
```


## Env variables & Parameters
In order to be able to bootstrap first block with specified users. Note: You can always construct it yourself. You need to specify following env variables so the server will be able to sign messages

OPENDB_SIGN_LOGIN=openplacereviews:test
OPENDB_SIGN_PK=<PK_WILL_BE_SPECIFIED>
