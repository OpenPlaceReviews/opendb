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
TO BE WRITTEN


# DBConsensusManager [service]
TO BE WRITTEN
