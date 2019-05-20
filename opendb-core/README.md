# OpenDB Java backend

This project represents OpenDB API and Spring boot server for configuration in 1 project. Below you can find documentation about services and data entities and also some information about Atomicity / Concurrency of operation and also Failure Resistance.

# OpObject, OpOperation, OpBlock [data entity]

These 3 classes represents main objects of blockchain.
OpBlock - represents a block of operations.
OpOperation - represents an operation that consists of newly created objects and references to deleted objects and other named objects. Operation always has a specific type.
OpObject - represents an object of specific type with freeform json representation.

## Concurrency
All object fields can be accessed concurrently after object becomes immutable. So non-immutable bojects should be shared between threads with care.

## Immutability
Once object becomes immutable it can't be changed anymore! All getters/setters should respect that rule, after object is immutable it can set cache values.

Once object is added to any blockchain (in-memory or db) it is validated and made immutable i.e. any operation in the queue and any block in any part of blockchain is immutable.

Cache fields are transferred by api but not stored in database.

# OpBlockchain - in-memory blockchain [data entity]

OpBlockchain - represents chain of blocks and consists of chain of superblocks. Basically OpBlockchain consists of List of Blocks and reference to parent OpBlockchain, 1 instance of OpBlockchain is called superblock. Superblock has a unique key "superblock hash" ( = "depth + hash of top block" ) + "parent superblock".

OpBlockchain has all operations to perform over blockchain in-memory, provides synchronization and guarantees validation consistency. So no operation or block could be added without proper validation. It uses stateless OpBlockchainRules (passed in constructor) to validate rules and create error messages.

Read more in Javadoc of the class.

## Concurrency and atomicity
This object is make full concurrent and it shouldn't produce any exception while accessing in multiple threads. However it doesn't guarantee consistency. In order to make a consistent call or a consistent validation, the method must be executed in synchronized method of OpBlockchain.

There is no synchronization going through all parent objects though it is not needed cause parent OpBlockchain is guaranteed to be unmodifiable. Once OpBlockchain is locked, it should provide consistent query results.

There are 4 atomic operation for modifications:
1. Atomic add operation - all caches reevaluated
2. Atomic block creation from added operations - no blockchain content changed.
3. Atomic change parent to equal parent - no blockchain content changed.
4. Atomic rebase operations - operations are deleted from the queue which are present in new parent (only for queue management)

## Immutability
There are extra methods to lock/unlock by user request and they do not overlap with states locked by other subchain or blockchain is broken i.e. atomic operation that should be executed without problem was executed with a crash.

# BlocksManager [service]
BlocksManager - provides all in 1 External API communication i.e. this project represents The Current active Blockchain for running OpenDB instance. All operations including adding operation to queue, creating a block or replicating a block via network, should go through that service.

## States
*BlocksManager* manages it state via single reference of OpBlockchain and proxies all database related operations to *DBConsensusManager*. It also supports 3 lock states: lock all operations, including adding operation to queue, lock replication from remote server, lock new block creation. BlocksManager could switch atomically  between different OpBlockchain references. So any Api client shouldn't store or cache reference to OpBlockchain and use as read-only information.

## Initialization
Initially BlocksManager will create database schema via *DBSchemaManager* and migrate data if it is needed. Later it will pass initialization process to *DBConsensusManager*. All block headers will be loaded from database via *DBConsensusManager* among these block headers the longest chain is selected which contains all db-stored-subchains (otherwise service won't start). Orphaned blocks could be persisted in database but they couldn't be part of db-stored-subchains. All blocks that belong to longest chain but not stored in db-stored-subchains (DBConsensusManager) will be loaded into memory and form runtime-stored-subchains OpBlockchains. On top of it all operations without any referenced blocks but present in database will be loaded into a queue following natural order of adding to the queue (by dbid).



## Atomicity / Concurrency
As of today all public modification methods are synchronized though this is done for extra safety. There are 3 main public methods: replicateBlock, createBlock, addOperation. In theory it is possible to desynchronize addOperation and createBlock but in practice there is no significant difference noticed yet. validateOperation also in theory could be changed non-synchronized method cause it is not even modification method. 
All management methods should be strictly synchronized cause they are executed manually and require special attention: clearQueue, rever1Block, deleteOperationFromQueue, revertSuperblock, unloadSuperblockFromDB, lockBlockchain. 

Read-only concurrency is guaranteed by OpBlockchain concurrency and atomicity.

# DBConsensusManager [service]
Represents an internal servcie for *BlocksManager* to deal with all DB-related operations. DB manager has 3 kind of main functions:
1. Load and initialize the main blockchain from database during startup.
2. Store and compact the tail of blockchain, to not deal with 1000 of blocks in memory. 
3. Insert all new valid blocks and operations into database, so the data is not lost between restarts.

## Store and compact tail of blockchain
In order to be able to support large blockchains, most of blockchain should be accessed from database. OpBlockchain provides interface for immutable subchains BlockDbAccessInterface, so database access could be provided. 

Db-Stored-blocks are constantly grouped by chains. The algorithm is simply following: <Size of Group 1>  + <Size of Group 2> >= <Size of Group 3>, then <Group 2> and <Group 3> could be combined. Once groups are combined the previous groups are no longer accessible. That's why running any validation during compacting operation is not consistent and could produce errors. As of today *BlocksManager* provides only synchronized methods to add-validate operations, so this is not an issue. On the other hand "compacting" could take significant time once the object count will be around 1 000 000. So, redesign to keep orphaned-db-stored-chains could be beneficial.

Compacting algorithm guarantees that number of subchains is log(N) - number of blocks. So, even with 100 000 000, the number of queries for searching 1 object by id won't exceed 10 queries and could be optimized within 1 SQL query.

## Atomicity and transactionality
Essentially it doesn't require atomicity cause it will be used solely by *BlocksManager* from synchornized methods. Though this service required to have proper transactionality and communication with *BlocksManager* that's why all atomic operations are wrapped with "START TRANSACTION/COMMIT/ROLLBACK". 

It is important to guarantee that if block has been deleted, all operations don't reference that block any more. The database is not supposed to have multiple write-clients (only 1 writer is allowed!) but it should support multiple read-clients. So keeping transactions consistent is important. *BlocksManager* will guarantee that there is no overlapping transaction by synchronized-methods.

# Extra services

## DBSchemaManager [service]
DB schema manager is a client side configuration for SQL-database to store all related to blockchain information. It is used during startup to create initial table layout and upgrade it between versions.

It is configurable to split objects into separate table by object types, so end-user can access it in the easy way. Though not all objects are accessible, only objects that belong to superblock-db (chains store in database). This parameter is configured in *DBConsensusManager* and by default is 32. So, objects in last 32 blocks are not accessible from sql database  object tables.

This is a stateless service and all local variables are initialized and final just after initialization.

## FileBackupManager [optional]
File backup manager stores all (even orphaned) blocks into gzipped-files grouped by 100 (configurable) blocks and 1000 per folder i.e. 10/200-300.gz (represents blocks with block id between 10200-10300 ) Any block received or created by the system will be stored there if it is configured.

## LogOperationService [optional]
Represents in memory only service to store and display log messages for 1 server run, it contains only important messages, so it is possible to get some highlights of top level errors. More details errors and messages are in the log files. Number of messages is limited.