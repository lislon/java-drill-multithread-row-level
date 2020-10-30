package ru.lislon;

public interface RowBlocker<T> {
    boolean tryRowLock(T key, Runnable block, long timeoutNs) throws InterruptedException;

    boolean tryGlobalLock(Runnable block, long timeoutNs) throws InterruptedException;

    boolean isGlobalLockActive();
}
