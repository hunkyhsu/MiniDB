package com.hunkyhsu.minidb.storage;

/**
 * Replacer - Interface for page replacement strategies

 * @author hunkyhsu
 */
public interface Replacer {

    /**
     * Get 1st frame from LinkedHashMap (LRU) and victim it
     * @return Frame ID or -1 if no victim
     */
    int victim();

    /**
     * 将 Frame 标记为不可淘汰（被 pin 住）
     *
     * 当 Page 的 pinCount > 0 时调用，表示该 Page 正在被使用
     *
     * @param frameId Frame ID
     */
    void pin(int frameId);

    /**
     * 将 Frame 标记为可淘汰（被 unpin）
     *
     * 当 Page 的 pinCount == 0 时调用，表示该 Page 可以被淘汰
     *
     * @param frameId Frame ID
     */
    void unpin(int frameId);

    /**
     * 获取当前可淘汰的 Frame 数量
     *
     * @return 可淘汰的 Frame 数量
     */
    int size();

}