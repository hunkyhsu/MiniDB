package com.hunkyhsu.minidb.access.heap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/3/23 20:32
 */
public class FreeSpaceMap {
    // PageId -> FreeSpace
    private final Map<Integer, Integer> freeSpaceMap = new ConcurrentHashMap<>();

    public int getAvailablePage(int requiredSize) {
        for (Map.Entry<Integer, Integer> entry : freeSpaceMap.entrySet()) {
            if (entry.getValue() >= requiredSize) { return entry.getKey(); }
        }
        return -1;
    }

    public void updateFreeSpace(int pageId, int freeSpace) {
        freeSpaceMap.put(pageId, freeSpace);
    }
}
