package com.hunkyhsu.minidb.engine.transaction;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 16:58
 */
public class TransactionManager {
    private final GlobalCommitLog clog;
    private final AtomicLong nextXmin;
    private final ConcurrentSkipListSet<Long> activeTxns;

    public TransactionManager(GlobalCommitLog clog) {
        this.clog = clog;
        this.nextXmin = new AtomicLong(1000L);
        this.activeTxns = new ConcurrentSkipListSet<>();
    }

    public long beginWriteTransaction() {
        long xmin = nextXmin.getAndIncrement();
        activeTxns.add(xmin);
        clog.setStatus(xmin, GlobalCommitLog.IN_PROGRESS);
        return xmin;
    }

    public void commitTransaction(long xmin) {
        clog.setStatus(xmin, GlobalCommitLog.COMMITTED);
        activeTxns.remove(xmin);
    }

    public void abortTransaction(long xmin) {
        clog.setStatus(xmin, GlobalCommitLog.ABORTED);
        activeTxns.remove(xmin);
    }

    public TransactionContext beginReadSnapshot(long currentTxnId) {
        long xmaxWatermark = nextXmin.get();
        long xminWatermark;
        try {
            xminWatermark = (activeTxns.isEmpty() ? xmaxWatermark : activeTxns.first());
        } catch (NoSuchElementException e) {
            xminWatermark = xmaxWatermark;
        }
        long[] activeArray = activeTxns.subSet(xminWatermark, true,
                        xmaxWatermark, false)
                        .stream().mapToLong(Long::longValue).toArray();
        return new TransactionContext(currentTxnId, xminWatermark, xmaxWatermark, activeArray, clog);
    }
}
