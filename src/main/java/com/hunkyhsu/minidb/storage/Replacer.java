package com.hunkyhsu.minidb.storage;

/**
 * Replacer - 页面替换策略接口
 *
 * 职责：
 * - 决定当 BufferPool 满时应该淘汰哪个 Frame
 * - 管理可淘汰的 Frame 列表（pinCount == 0 的 Frame）
 * - 提供线程安全的操作接口
 *
 * 实现策略：LRU (Least Recently Used)
 *
 * @author hunkyhsu
 */
public interface Replacer {

    /**
     * 选择一个 Frame 进行淘汰（Evict）
     *
     * 返回最久未使用（Least Recently Used）的 Frame ID
     *
     * @return Frame ID，如果没有可淘汰的 Frame 则返回 -1
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