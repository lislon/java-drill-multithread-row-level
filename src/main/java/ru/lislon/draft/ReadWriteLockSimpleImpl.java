package ru.lislon.draft;

import ru.lislon.RowBlocker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockSimpleImpl<T> implements RowBlocker<T> {
    private final ConcurrentHashMap<T, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(false);


    @Override
    public boolean tryRowLock(T key, Runnable block, long timeoutNs) throws InterruptedException {
        globalLock.readLock().lock();
        try {
            ReentrantLock lock = locks.computeIfAbsent(key, id -> new ReentrantLock());
            if (lock.tryLock(timeoutNs, TimeUnit.NANOSECONDS)) {
                try {
                    block.run();
                    return true;
                } finally {
                    lock.unlock();
                }
            }
            return false;
        } finally {
            globalLock.readLock().unlock();
        }
    }

    @Override
    public boolean tryGlobalLock(Runnable block, long timeoutNs) throws InterruptedException {
        if (!globalLock.writeLock().tryLock(timeoutNs, TimeUnit.NANOSECONDS)) {
            return false;
        }
        try {
            block.run();
            return true;
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isGlobalLocked() {
        return false;
    }

}
