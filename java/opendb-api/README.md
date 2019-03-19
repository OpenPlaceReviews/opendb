## Concurrency & mutability

## OpObject, OpOperation, OpBlock

These 3 classes represents main objects of blockchain.
OpBlock - represents a block of operations.
OpOperation - represents an operation that consists of newly created objects and references to deleted objects and other named objects. Operation always has a specific type.
OpObject - represents an object of specific type with freeform json representation.

# Concurrency
All object fields can be accessed concurrently after object becomes immutable. So non-immutable bojects should be shared between threads with care.

Once object becomes immutable it can't be changed anymore! All getters/setters should respect that rule, after object is immutable it can set cache values.




# OpBlockchain - in-memory blockchain
