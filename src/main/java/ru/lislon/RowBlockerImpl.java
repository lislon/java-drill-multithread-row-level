package ru.lislon;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class RowBlockerImpl<T> implements RowBlocker<T> {
    /**
     * When amount of locks held by single thread exceed this threshold,
     * a global lock will be escalated for current thread.
     */
    public static final int THRESHOLD_MAX_LOCKS_HELD_BY_THREAD = 10;

    /**
     * Hold reentrant blocks for each row-level key.
     */
    private final ConcurrentHashMap<T, ReentrantLock> rowLocks = new ConcurrentHashMap<>();
    /**
     * Reentrant block for global lock.
     */
    private final ReentrantLock globalLock = new ReentrantLock(false);
    /**
     * Condition to notify threads waiting for row-level blocks, that global lock in ended.
     */
    private final Condition globalLockIsFreeCond = globalLock.newCondition();
    /**
     * Count of row locks held by single thread.
     */
    private final ThreadLocal<Integer> lockDepth = new ThreadLocal<>();
    /**
     * Flag indicating that global lock is initiated.
     *
     * true means that global lock is either:
     *   - some thread took global lock and waits till row-level locks will finish their jobs
     *   - some thread took global lock and execution is in process
     *   - row-level blocks allowed only to release existing locks, but not take a new ones.
     */
    private volatile boolean isGlobalLockStarted = false;

    private enum LockType { GLOBAL, ROW }

    /**
     * Executes given block with guarantee that at no thread will hold block with same key.
     *
     * @param key non-nullable key
     * @param block Block to execute
     * @param timeoutNs Timeout in nanoseconds
     * @return true if lock was successfully taken and block was executed without exceptions, false otherwise.
     * @throws InterruptedException when thread was interrupted
     * @throws IllegalArgumentException when key or block is null
     */
    @Override
    public boolean tryRowLock(T key, Runnable block, long timeoutNs) throws InterruptedException {
        if (key == null) {
            throw new IllegalArgumentException("Key should be non-nullable");
        }
        if (block == null) {
            throw new IllegalArgumentException("Block should be non-nullable");
        }

        long deadLine = System.nanoTime() + timeoutNs;

        while (deadLine - System.nanoTime() > 0) {

            LockType thisRowLockType;
            if (!this.isGlobalLockStarted) {
                thisRowLockType = LockType.ROW;
            } else if (this.globalLock.isHeldByCurrentThread()) {
                thisRowLockType = LockType.GLOBAL;
            } else if (getLockDepth() == 0) {
                waitForGlobalLockToEnd(deadLine);
                continue;
            } else {
                // When global lock is started, allow threads holding any row locks to finish their job
                thisRowLockType = LockType.ROW;
            }

            if (thisRowLockType == LockType.ROW) {

                if (getLockDepth() > THRESHOLD_MAX_LOCKS_HELD_BY_THREAD) {
                    return tryGlobalLock(block, deadLine - System.nanoTime());
                }

                ReentrantLock rowLock = rowLocks.computeIfAbsent(key, id -> new ReentrantLock());

                if (rowLock.tryLock(deadLine - System.nanoTime(), TimeUnit.NANOSECONDS)) {
                    try {
                        if (this.isGlobalLockStarted) {
                            // looks like global lock has been called during taking our rowLock.
                            // Start over, as there might be global lock running
                            continue;
                        }
                        // we are safe here, since we know that globalLock was false AFTER we took took our rowLock
                        // So new globalLocks will wait till we unblock rowLock
                        countDepthAndExecute(block);
                        return true;
                    } finally {
                        rowLock.unlock();
                    }
                }
            } else if (thisRowLockType == LockType.GLOBAL) {
                block.run();
                return true;
            }
        }
        return false;
    }

    /**
     * Executes given block with guarantee that at no thread will hold any other row blocks.
     *
     * @param block Block to execute.
     * @param timeoutNs Timeout in nanoseconds.
     * @return true if lock was successfully taken and block was executed without exceptions, false otherwise.
     * @throws InterruptedException when thread was interrupted
     * @throws IllegalArgumentException when block is null
     */
    @Override
    public boolean tryGlobalLock(Runnable block, long timeoutNs) throws InterruptedException {
        if (block == null) {
            throw new IllegalArgumentException("Block should be non-nullable");
        }

        long deadLine = System.nanoTime() + timeoutNs;

        if (!globalLock.tryLock(timeoutNs, TimeUnit.NANOSECONDS)) {
            return false;
        }

        try {
            isGlobalLockStarted = true;

            if (!waitForAllRowLocks(deadLine)) {
                return false;
            }

            block.run();
            return true;

        } finally {
            isGlobalLockStarted = false;
            globalLockIsFreeCond.signalAll();
            globalLock.unlock();
        }
    }

    @Override
    public boolean isGlobalLocked() {
        return isGlobalLockStarted;
    }

    /**
     * Waits until all non-local thread locks are unlocked.
     *
     * During execution of this method, isGlobalLockStarted variable will be always true,
     * and rowLocks will remain no threads are able to add new rowLocks entities or lock on existing one.
     */
    private boolean waitForAllRowLocks(long deadLine) throws InterruptedException {
        Iterator<Map.Entry<T, ReentrantLock>> it = rowLocks.entrySet().iterator();
        while (it.hasNext()) {
            var rowLock = it.next().getValue();

            if (!rowLock.isHeldByCurrentThread()) {
                while (rowLock.isLocked()) {
                    // spin wait
                    if (deadLine - System.nanoTime() < 0) {
                        return false;
                    }
                }

                // Possible improvement: Implement custom WeakValueConcurrentHashMap for cleaning up of unused locks during non-global locks
                //       (like guava's https://guava.dev/releases/18.0/api/docs/com/google/common/collect/MapMaker.html)
                it.remove();
            }
        }
        return true;
    }

    /**
     * Waits till globalLock will change it state from busy to free.
     *
     * @param deadLine
     * @throws InterruptedException
     */
    private void waitForGlobalLockToEnd(long deadLine) throws InterruptedException {
        if (this.globalLock.tryLock(deadLine - System.nanoTime(), TimeUnit.NANOSECONDS)) {
            try {
                if (this.isGlobalLockStarted) {
                    globalLockIsFreeCond.await(deadLine - System.nanoTime(), TimeUnit.NANOSECONDS);
                }
            } catch (InterruptedException e) {
                // possibility that globalLockIsFree is already free not under lock
            } finally {
                this.globalLock.unlock();
            }
        }
    }

    private int getLockDepth() {
        if (lockDepth.get() != null) {
            return lockDepth.get();
        }
        return 0;
    }

    private void countDepthAndExecute(Runnable block)  {
        int depth = getLockDepth();

        try {
            this.lockDepth.set(depth + 1);
            block.run();
        } finally {
            this.lockDepth.set(depth);
        }
    }
}
