package ru.lislon.benchmark;

import org.openjdk.jmh.annotations.*;
import ru.lislon.TestGlobals;
import ru.lislon.draft.OnlyRowBlockImpl;


import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;


public class OnlyRowBlockImplPerf {

    public static final int CACHES_ENTRIES = 100;

    @State(Scope.Benchmark)
    public static class MyState {
        OnlyRowBlockImpl<Integer> rb = new OnlyRowBlockImpl<>();
        HashMap<Integer, Integer> data = new HashMap<>();

        @Setup
        public void setup() {
            for (int i = 0; i < CACHES_ENTRIES; i++) {
                data.put(i, 0);
            }
        }
    }

    @Benchmark
    @Group("rowBlock")
    @GroupThreads(8)
    public boolean rowBlock(MyState state) throws InterruptedException {
        int cacheId = ThreadLocalRandom.current().nextInt(CACHES_ENTRIES);
        return state.rb.tryRowLock(cacheId, () -> {
            state.data.compute(cacheId, (k, v) -> v + 1);
        }, TestGlobals.TIMEOUT_NS);
    }

}