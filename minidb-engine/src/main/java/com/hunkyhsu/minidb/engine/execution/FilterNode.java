package com.hunkyhsu.minidb.engine.execution;

import java.util.function.Predicate;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/5 21:13
 */
public class FilterNode implements DbIterator {
    private final DbIterator childNode;
    private final Predicate<Long> condition;

    public FilterNode(DbIterator childNode, Predicate<Long> condition) {
        this.childNode = childNode;
        this.condition = condition;
    }

    public void open() {
        childNode.open();
    }
    public long next() {
        while (true) {
            long tuplePointer = childNode.next();
            if (tuplePointer == DbIterator.EOF) { return DbIterator.EOF; }
            if (condition.test(tuplePointer)) { return tuplePointer; }
        }
    }
    public void close() {
        childNode.close();
    }
}
