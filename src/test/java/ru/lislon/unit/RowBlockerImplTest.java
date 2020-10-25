package ru.lislon.unit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Timeout;
import ru.lislon.RowBlockerImpl;
import ru.lislon.RowBlocker;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static ru.lislon.TestGlobals.TIMEOUT_NS;

@Timeout(time = 1, timeUnit = TimeUnit.MINUTES)
public class RowBlockerImplTest {

    public static final int CACHES_ENTRIES = 20;
    public static final int ROW_LOCKS_COUNT = 10000;
    public static final int GLOBAL_LOCKS_COUNT = 100;

    HashMap<Integer, Integer> data = new HashMap<>();
    RowBlocker<Integer> r = new RowBlockerImpl<>();
    ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2);

    @BeforeEach
    void setup() {
        for (int i = 0; i < CACHES_ENTRIES; i++) {
            data.put(i, 0);
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
        assertThrows(IllegalArgumentException.class, () -> r.tryRowLock(null, () -> {}, TIMEOUT_NS));
    }

    @Test
    void parallelIncrements() throws Exception {
        for (int i = 0; i < ROW_LOCKS_COUNT; i++) {
            pool.submit(() -> {
                int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
                try {
                    r.tryRowLock(cacheId, () -> slowPlusPlus(cacheId), TIMEOUT_NS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.DAYS);

        assertSumDataWillBe(ROW_LOCKS_COUNT);
    }

    private void assertSumDataWillBe(int expected) {
        int sum = data.values().stream().mapToInt(x -> x).sum();
        assertEquals(expected, sum);
    }

    @Test
    void reentrantLocking() throws Exception {
        ExecutorService s = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < ROW_LOCKS_COUNT; i++) {
            s.submit(() -> {
                try {
                    int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
                    r.tryRowLock(cacheId, () -> {
                        try {
                            r.tryRowLock(cacheId, () -> {
                                slowPlusPlus(cacheId);
                            }, TIMEOUT_NS);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }, TIMEOUT_NS);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
        }

        s.shutdown();
        s.awaitTermination(1, TimeUnit.DAYS);

        assertSumDataWillBe(ROW_LOCKS_COUNT);
    }

    @Test
    void timoutLock() throws Exception {
        ExecutorService s = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        s.submit(() -> {
            try {
                r.tryRowLock(1, () -> {
                    try {
                        Thread.sleep(10000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, TIMEOUT_NS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread.sleep(1000);
        boolean isSuccess = r.tryRowLock(1, () -> {}, 1000);
        assertFalse(isSuccess);
    }

    @Test
    void parallelIncrementsWithGlobalLocks() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
            return r.tryRowLock(cacheId, () -> {
                slowPlusPlus(cacheId);
            }, TIMEOUT_NS);
        });

        var globalLocksTasks = runInParallel(GLOBAL_LOCKS_COUNT, () -> {
            return r.tryGlobalLock(() -> {
                data.forEach((key, oldValue) -> {
                    slowPlusPlus(key);
                });
            }, TIMEOUT_NS);
        });

        assertShuffledTasksAreDone(rowLocksTasks, globalLocksTasks);
        assertEquals(ROW_LOCKS_COUNT + GLOBAL_LOCKS_COUNT * data.size(), data.values().stream().mapToInt(x -> x).sum());
    }

    private void deepBlock(int depth, AtomicBoolean didYouSeeGlobalLock) {
        try {
            r.tryRowLock(depth, () -> {
                if (depth <= 0) {
                    slowPlusPlus(1);
                    didYouSeeGlobalLock.set(r.isGlobalLocked());
                } else {
                    deepBlock(depth - 1, didYouSeeGlobalLock);
                }
            }, TIMEOUT_NS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void escalation() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
            return r.tryRowLock(cacheId, () -> {
                slowPlusPlus(cacheId);
            }, TIMEOUT_NS);
        });
        AtomicBoolean didYouSeeGlobalLock = new AtomicBoolean(false);

        var escalatedTask = runInParallel(1, () -> {
            deepBlock(RowBlockerImpl.THRESHOLD_MAX_LOCKS_HELD_BY_THREAD + 1, didYouSeeGlobalLock);
            return true;
        });

        assertShuffledTasksAreDone(rowLocksTasks, escalatedTask);
        assertTrue(didYouSeeGlobalLock.get());
        assertEquals(ROW_LOCKS_COUNT + 1, data.values().stream().mapToInt(x -> x).sum());
    }


    @Test
    void callRowLockWhileInGlobalLock() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
            return r.tryRowLock(cacheId, () -> {
                slowPlusPlus(cacheId);
            }, TIMEOUT_NS);
        });

        var globalLocksTasks = runInParallel(GLOBAL_LOCKS_COUNT, () -> {
            return r.tryGlobalLock(() -> {
                int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
                try {
                    r.tryRowLock(cacheId, () -> {
                        try {
                            r.tryGlobalLock(() -> {
                                slowPlusPlus(cacheId);
                            }, TIMEOUT_NS);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }, TIMEOUT_NS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }, TIMEOUT_NS);
        });


        assertShuffledTasksAreDone(rowLocksTasks, globalLocksTasks);
        assertEquals(ROW_LOCKS_COUNT + GLOBAL_LOCKS_COUNT, data.values().stream().mapToInt(x -> x).sum());
    }


    @Test
    void callGlobalLockWhileInLocalLock() throws Exception {
        var rowLocksTasks = runInParallel(ROW_LOCKS_COUNT, () -> {
            int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
            return r.tryRowLock(cacheId, () -> {
                slowPlusPlus(cacheId);
                System.out.println("\t.");
            }, TIMEOUT_NS);
        });

        var globalLocksTasks = runInParallel(GLOBAL_LOCKS_COUNT, () -> {
            return r.tryGlobalLock(() -> {
                System.out.println("[");
                int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
                try {
                    r.tryRowLock(cacheId, () -> {
                        slowPlusPlus(cacheId);
                        System.out.println("\ti");
                    }, TIMEOUT_NS);
                    System.out.println("]");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }, TIMEOUT_NS);
        });

        assertShuffledTasksAreDone(rowLocksTasks, globalLocksTasks);
        assertEquals(ROW_LOCKS_COUNT + GLOBAL_LOCKS_COUNT, data.values().stream().mapToInt(x -> x).sum());
    }


    private void slowPlusPlus(int cacheId) {
        int oldValue = data.get(cacheId);

        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            fail(e);
        }
        data.put(cacheId, oldValue + 1);
    }

    private void assertShuffledTasksAreDone(Stream<Callable<Boolean>> rowLocksTasks, Stream<Callable<Boolean>> globalLocksTasks)  throws InterruptedException, ExecutionException {

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

}
