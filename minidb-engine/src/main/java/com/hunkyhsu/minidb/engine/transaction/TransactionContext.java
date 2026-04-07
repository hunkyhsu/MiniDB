package com.hunkyhsu.minidb.engine.transaction;

import java.util.Arrays;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 16:58
 */
public class TransactionContext {
    private final long currentTxnId;
    private final long xminWatermark;
    private final long xmaxWatermark;
    private final long[] activeTxns;
    private final GlobalCommitLog cLog;

    public TransactionContext(long currentTxnId, long xminWatermark, long xmaxWatermark,
                              long[] activeTxns, GlobalCommitLog cLog) {
        this.currentTxnId = currentTxnId;
        this.xminWatermark = xminWatermark;
        this.xmaxWatermark = xmaxWatermark;
        if (!isSorted(activeTxns)) {
            Arrays.sort(activeTxns);
        }
        this.activeTxns = activeTxns;
        this.cLog = cLog;
    }

    public boolean isVisible(long xmin) {
        if (xmin == currentTxnId) { return true; }
        else if (xmin >= xmaxWatermark) { return false; }
        else if (xmin < xminWatermark) {
            int status = cLog.getStatus(xmin);
            return status == GlobalCommitLog.COMMITTED;
        }
        else {
            if (Arrays.binarySearch(activeTxns, xmin) >= 0) { return false;}
            else {
                int status = cLog.getStatus(xmin);
                return status == GlobalCommitLog.COMMITTED;
            }
        }
    }

    private static boolean isSorted(long[] txnArray) {
        for (int i = 1; i < txnArray.length; i++) {
            if (txnArray[i] < txnArray[i - 1]) {
                return false;
            }
        }
        return true;
    }
}
