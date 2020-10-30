package ru.lislon.unit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.lislon.RowBlocker;
import ru.lislon.RowBlockerImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static ru.lislon.TestGlobals.TIMEOUT_NS;

public class RowBlockerImplTest {
    public static final int CACHES_ENTRIES = 20;
    public static final int ROW_LOCKS_COUNT = 10000;
    public static final int GLOBAL_LOCKS_COUNT = 100;

    HashMap<Integer, Integer> payload = new HashMap<>();
    RowBlocker<Integer> r = new RowBlockerImpl<>();
    ExecutorService pool = Executors.newFixedThreadPool(8);

    @BeforeEach
    void setup() {
        for (int i = 0; i < CACHES_ENTRIES; i++) {
            payload.put(i, 0);
        }
    }

    @Test
    void illegalArgsWhenBlockIsNullForGlobal() {
        assertThrows(IllegalArgumentException.class, () -> r.tryGlobalLock(null, TIMEOUT_NS));
    }

    @Test
    void illegalArgsWhenBlockIsNullForRow() {
        assertThrows(IllegalArgumentException.class, () -> r.tryRowLock(1, null, TIMEOUT_NS));
    }

    @Test
    void illegalArgsWhenKeyIsNull() {
        assertThrows(IllegalArgumentException.class, () -> r.tryRowLock(null, () -> {
        }, TIMEOUT_NS));
    }

    @Test
    @DisplayName("Row level blocks prevents lost updates")
    void parallelIncrements() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = getRandomKeyId();
            try {
                return r.tryRowLock(cacheId, () -> emulateSomeWork(cacheId), TIMEOUT_NS);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });

        assertShuffledTasksAreDone(rowLocksTasks, Stream.empty());
        assertPayloadSumEquals(ROW_LOCKS_COUNT);
    }

    @Test
    @DisplayName("Row level locks can be taken several times by single thread")
    void rowLockIsReentrant() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            try {
                int cacheId = getRandomKeyId();
                return r.tryRowLock(cacheId, () -> {
                    try {
                        r.tryRowLock(cacheId, () -> emulateSomeWork(cacheId), TIMEOUT_NS);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, TIMEOUT_NS);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });

        assertShuffledTasksAreDone(rowLocksTasks, Stream.empty());
        assertPayloadSumEquals(ROW_LOCKS_COUNT);
    }

    @Test
    @DisplayName("Row level locks will give up after timeout")
    void timeoutLock() throws Exception {
        CountDownLatch rowIsBlocked = new CountDownLatch(1);
        pool.submit(() -> r.tryRowLock(1, () -> {
            try {
                rowIsBlocked.countDown();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, TIMEOUT_NS));

        pool.shutdown();
        rowIsBlocked.await(TIMEOUT_NS, TimeUnit.NANOSECONDS);
        assertFalse(r.tryRowLock(1, () -> {}, 1000));
    }

    @Test
    @DisplayName("Interleaved row and global locks are not deadlocks")
    void parallelIncrementsWithGlobalLocks() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = getRandomKeyId();
            return r.tryRowLock(cacheId, () -> emulateSomeWork(cacheId), TIMEOUT_NS);
        });

        var globalLocksTasks = runInParallel(GLOBAL_LOCKS_COUNT, () ->
                r.tryGlobalLock(() -> {
                    payload.forEach((key, oldValue) -> emulateSomeWork(key));
                }, TIMEOUT_NS));

        assertShuffledTasksAreDone(rowLocksTasks, globalLocksTasks);
        assertPayloadSumEquals(ROW_LOCKS_COUNT + GLOBAL_LOCKS_COUNT * payload.size());
    }

    private void deepBlock(int depth, AtomicBoolean didYouSeeGlobalLock) {
        try {
            r.tryRowLock(depth, () -> {
                if (depth <= 0) {
                    emulateSomeWork(1);
                    didYouSeeGlobalLock.set(r.isGlobalLockActive());
                } else {
                    deepBlock(depth - 1, didYouSeeGlobalLock);
                }
            }, TIMEOUT_NS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("When one thread locks more then N row locks it escalates to global lock")
    void escalation() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = getRandomKeyId();
            return r.tryRowLock(cacheId, () -> {
                emulateSomeWork(cacheId);
            }, TIMEOUT_NS);
        });
        AtomicBoolean isGlobalLockSeen = new AtomicBoolean(false);

        var escalatedTask = runInParallel(1, () -> {
            deepBlock(RowBlockerImpl.THRESHOLD_MAX_LOCKS_HELD_BY_THREAD + 1, isGlobalLockSeen);
            return true;
        });

        assertShuffledTasksAreDone(rowLocksTasks, escalatedTask);
        assertTrue(isGlobalLockSeen.get());
        assertPayloadSumEquals(ROW_LOCKS_COUNT + 1);
    }


    @Test
    @DisplayName("Row lock should work if called inside global lock")
    void rowLockShouldWorkInsideGlobalLock() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = getRandomKeyId();
            return r.tryRowLock(cacheId, () -> {
                emulateSomeWork(cacheId);
            }, TIMEOUT_NS);
        });

        var globalLocksTasks = runInParallel(GLOBAL_LOCKS_COUNT, () -> {
            return r.tryGlobalLock(() -> {
                int cacheId = getRandomKeyId();
                try {
                    r.tryRowLock(cacheId, () -> {
                        emulateSomeWork(cacheId);
                    }, TIMEOUT_NS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }, TIMEOUT_NS);
        });


        assertShuffledTasksAreDone(rowLocksTasks, globalLocksTasks);
        assertPayloadSumEquals(ROW_LOCKS_COUNT + GLOBAL_LOCKS_COUNT);
    }


    @Test
    @DisplayName("Global lock should work if called inside row-level lock")
    void rowLockWorksInsideGlobalLock() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = getRandomKeyId();
            return r.tryRowLock(cacheId, () -> {
                emulateSomeWork(cacheId);
            }, TIMEOUT_NS);
        });

        var globalLocksTasks = runInParallel(GLOBAL_LOCKS_COUNT, () ->
                r.tryGlobalLock(() -> {
                    int cacheId = getRandomKeyId();
                    try {
                        r.tryRowLock(cacheId, () -> {
                            emulateSomeWork(cacheId);
                        }, TIMEOUT_NS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }, TIMEOUT_NS));

        assertShuffledTasksAreDone(rowLocksTasks, globalLocksTasks);
        assertPayloadSumEquals(ROW_LOCKS_COUNT + GLOBAL_LOCKS_COUNT);
    }

    private int getRandomKeyId() {
        return ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
    }

    void lockRowRecursivelyThenRun(int depth, Integer key, Runnable block) {
        if (depth <= 0) {
            block.run();
        } else {
            try {
                r.tryRowLock(key, () -> lockRowRecursivelyThenRun(depth - 1, key, block), TIMEOUT_NS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    @DisplayName("Two parallel escalations to global lock should not deadlock")
    void twoParallelEscalations() throws Exception {
        CountDownLatch l = new CountDownLatch(2);
        AtomicInteger id = new AtomicInteger(0);
        int[] calledTimes = {0};

        for (int i = 0; i < 2; i++) {
            pool.submit(() -> {
                        int keyId = id.getAndAdd(1);
                        int depth = RowBlockerImpl.THRESHOLD_MAX_LOCKS_HELD_BY_THREAD;
                        lockRowRecursivelyThenRun(depth, keyId, () -> {
                                    try {
                                        l.countDown();
                                        l.await(TIMEOUT_NS, TimeUnit.NANOSECONDS);
                                        lockRowRecursivelyThenRun(2, keyId, () -> {
                                            calledTimes[0]++;
                                        });
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                        );
                        return true;
                    }
            );
        }

        pool.shutdown();
        assertTrue(pool.awaitTermination(TIMEOUT_NS, TimeUnit.NANOSECONDS));
        assertEquals(2, calledTimes[0]);
    }

    @Test
    @DisplayName("Thread will release escalation mode once it deep level drops below threshold")
    void threadReleasesEscalationLock() throws Exception {
        boolean[] inEscalation = {false};
        boolean[] afterEscalation = {false};
        lockRowRecursivelyThenRun(RowBlockerImpl.THRESHOLD_MAX_LOCKS_HELD_BY_THREAD, 1,
                () -> {
                    try {
                        r.tryRowLock(2, () -> {
                            emulateSomeWork(1);
                            inEscalation[0] = r.isGlobalLockActive();
                        }, TIMEOUT_NS);
                        afterEscalation[0] = r.isGlobalLockActive();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        assertTrue(inEscalation[0]);
        assertFalse(afterEscalation[0]);
    }

    private void emulateSomeWork(int cacheId) {
        int oldValue = payload.get(cacheId);

        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            fail(e);
        }
        payload.put(cacheId, oldValue + 1);
    }

    private void assertShuffledTasksAreDone(Stream<Callable<Boolean>> rowLocksTasks,
                                            Stream<Callable<Boolean>> globalLocksTasks)
            throws InterruptedException, ExecutionException {

        List<Callable<Boolean>> list = Stream.concat(rowLocksTasks, globalLocksTasks).collect(Collectors.toList());
        Collections.shuffle(list);
        List<Future<Boolean>> futures = pool.invokeAll(list);

        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.MINUTES));

        for (Future<Boolean> f : futures) {
            assertTrue(f.get());
        }
    }

    private Stream<Callable<Boolean>> runInParallel(int count, Callable<Boolean> callable) {
        return IntStream.range(0, count)
                .mapToObj((i) -> () -> {
                    try {
                        return callable.call();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                });
    }

    private void assertPayloadSumEquals(int expected) {
        assertEquals(expected, payload.values().stream().mapToInt(x -> x).sum());
    }
}
