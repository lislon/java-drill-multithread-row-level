# Java multithread task

EntityLocker
------------

The task is to create a reusable utility class that provides synchronization mechanism similar to row-level DB locking.

The class is supposed to be used by the components that are responsible for managing storage and caching of different type of entities in the application. EntityLocker itself does not deal with the entities, only with the IDs (primary keys) of the entities.

Requirements:

1. EntityLocker should support different types of entity IDs.
2. EntityLocker’s interface should allow the caller to specify which entity does it want to work with (using entity ID), and designate the boundaries of the code that should have exclusive access to the entity (called “protected code”).
3. For any given entity, EntityLocker should guarantee that at most one thread executes protected code on that entity. If there’s a concurrent request to lock the same entity, the other thread should wait until the entity becomes available.
4. EntityLocker should allow concurrent execution of protected code on different entities.


Bonus requirements (optional):

 - [X] I. Allow reentrant locking.
 - [X] II. Allow the caller to specify timeout for locking an entity.
 - [ ] III. Implement protection from deadlocks (but not taking into account possible locks outside EntityLocker).
 - [X] IV. Implement global lock. Protected code that executes under a global lock must not execute concurrently with any other protected code.
 - [X] V. Implement lock escalation. If a single thread has locked too many entities, escalate its lock to be a global lock.


### Requirements

 * Gradle 5.5+
 
 To update gradle use:
 ```
./gradlew wrapper --gradle-version 6.7
```



Benchmark          Mode  Cnt      Score      Error   Units
Test1.testMethod  thrpt   10  18642,339 ▒ 1699,093  ops/ms
Test2.testMethod  thrpt   10   2944,317 ▒  180,470  ops/ms


Benchmark          Mode  Cnt         Score        Error  Units
Test1.testMethod  thrpt   25  27975652,168 ▒ 424948,772  ops/s
Test2.testMethod  thrpt   25  20408446,138 ▒ 292869,971  ops/s
Test3.testMethod  thrpt   25  26851563,986 ▒ 370466,601  ops/s
