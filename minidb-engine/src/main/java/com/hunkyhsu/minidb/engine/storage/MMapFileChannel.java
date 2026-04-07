package com.hunkyhsu.minidb.engine.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/3 20:43
 */
public class MMapFileChannel implements AutoCloseable {
    private final MappedByteBuffer mappedBuffer;

    public MMapFileChannel(String filePath, int maxFileSize) throws IOException {
        Path path = Paths.get(filePath);
        // Ensure the ParentPath exists
        Path parentPath = path.getParent();
        if (parentPath != null) {
            File parentFile = parentPath.toFile();
            if (!Files.exists(parentPath) && !parentFile.mkdirs()) {
                throw new IOException("Can not create directory: " + parentPath);
            }
        }
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            if (raf.length() < maxFileSize) {raf.setLength(maxFileSize);}
            FileChannel fileChannel = raf.getChannel();
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileSize);
        } catch (IOException e) {
            throw new IOException("Can not open file: " + path);
        }
    }

    public long readXmin(long pointer) {
        int pageId = TuplePointer.getPageId(pointer);
        int offset = TuplePointer.getOffset(pointer);
        int absOffset = pageId * PageLayout.PAGE_SIZE + offset + PageLayout.XMIN_OFFSET;
        return mappedBuffer.getLong(absOffset);
    }

    public int readPayloadLength(long pointer) {
        int pageId = TuplePointer.getPageId(pointer);
        int offset = TuplePointer.getOffset(pointer);
        int absOffset = pageId * PageLayout.PAGE_SIZE + offset + PageLayout.LEN_OFFSET;
        return mappedBuffer.getInt(absOffset);
    }

    public byte[] readPayload(long pointer) {
        int lenPayload = readPayloadLength(pointer);
        int pageId = TuplePointer.getPageId(pointer);
        int offset = TuplePointer.getOffset(pointer);
        int absOffset = pageId * PageLayout.PAGE_SIZE + offset + PageLayout.HEADER_SIZE;
        byte[] payload = new byte[lenPayload];
        mappedBuffer.get(absOffset, payload, 0, lenPayload);
        return payload;
    }

    public void writeTuple(long pointer, long xmin, byte[] payload) {
        int pageId = TuplePointer.getPageId(pointer);
        int offset = TuplePointer.getOffset(pointer);
        int baseOffset = pageId * PageLayout.PAGE_SIZE + offset;
        mappedBuffer.putLong(baseOffset + PageLayout.XMIN_OFFSET, xmin);
        mappedBuffer.putInt(baseOffset + PageLayout.LEN_OFFSET, payload.length);
        mappedBuffer.put(baseOffset + PageLayout.HEADER_SIZE, payload);
    }

    public void close() throws IOException {
        mappedBuffer.force();
        try {
            Method getCleanerMethod = mappedBuffer.getClass().getMethod("cleaner");
            getCleanerMethod.setAccessible(true);
            Object cleaner = getCleanerMethod.invoke(mappedBuffer);
            if (cleaner != null) {
                Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            System.err.println("Warning: Failed to manually unmap buffer: " + e.getMessage());
        }
    }
}
