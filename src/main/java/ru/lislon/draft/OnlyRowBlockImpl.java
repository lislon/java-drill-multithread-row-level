package ru.lislon.draft;

import ru.lislon.RowBlocker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class OnlyRowBlockImpl<T> implements RowBlocker<T> {
    private ConcurrentHashMap<T, ReentrantLock> locks = new ConcurrentHashMap<>();

    @Override
    public boolean tryRowLock(T key, Runnable block, long timeoutNs) throws InterruptedException {
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
    }

    @Override
    public boolean tryGlobalLock(Runnable block, long timeoutNs) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isGlobalLocked() {
        return false;
    }
}
