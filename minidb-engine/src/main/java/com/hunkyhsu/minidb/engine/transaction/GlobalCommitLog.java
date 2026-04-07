package com.hunkyhsu.minidb.engine.transaction;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 16:58
 */
public class GlobalCommitLog {
    public static final int IN_PROGRESS = 0b00;
    public static final int COMMITTED = 0b01;
    public static final int ABORTED = 0b10;

    public static final int BITS_PER_ENTRY = 2;
    public static final int ENTRY_PER_LONG = Long.SIZE / BITS_PER_ENTRY;

    private final AtomicLongArray statesArray;

    public GlobalCommitLog(int maxTransactions) {
        int statesArraySize = (maxTransactions / ENTRY_PER_LONG) + 1;
        statesArray = new AtomicLongArray(statesArraySize);
    }

    public int getStatus(long xmin) {
        int stateIndex = (int) (xmin / ENTRY_PER_LONG);
        int stateOffset = (int) ((xmin % ENTRY_PER_LONG) * BITS_PER_ENTRY);
        long states = statesArray.get(stateIndex);
        return (int)((states >>> stateOffset) & 0b11);
    }

    public void setStatus(long xmin, int status) {
        int stateIndex = (int) (xmin / ENTRY_PER_LONG);
        int stateOffset = (int) ((xmin % ENTRY_PER_LONG) * BITS_PER_ENTRY);
        long currentStates;
        long newStates;
        do {
            currentStates = statesArray.get(stateIndex);
            long clearMask = ~(0b11L << stateOffset);
            newStates = (clearMask & currentStates) | (((long) status) << stateOffset);
        } while (!statesArray.compareAndSet(stateIndex, currentStates, newStates));
    }
}
