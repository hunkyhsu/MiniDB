package com.hunkyhsu.minidb.config;

import com.hunkyhsu.minidb.engine.MiniDbEngine;
import com.hunkyhsu.minidb.engine.catalog.CatalogManager;
import com.hunkyhsu.minidb.engine.storage.AppendOnlyTableStore;
import com.hunkyhsu.minidb.engine.storage.MMapFileChannel;
import com.hunkyhsu.minidb.engine.transaction.GlobalCommitLog;
import com.hunkyhsu.minidb.engine.transaction.TransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Paths;

@Configuration
public class EngineConfig {
    @Value("${engine.db-path: ./data/minidb.dat}")
    private String dbPath;
    @Value("${engine.file-size: 104857600}")
    private int fileSize;
    @Value("${engine.max-transactions: 100000}")
    private int maxTransactions;

    @Bean(destroyMethod = "close")
    public MMapFileChannel mmapFileChannel() throws IOException {
        Paths.get(dbPath).getParent().toFile().mkdirs();
        return new MMapFileChannel(dbPath, fileSize);
    }

    @Bean
    public AppendOnlyTableStore appendOnlyTableStore(MMapFileChannel channel) {
        return new AppendOnlyTableStore(channel);
    }

    @Bean
    public GlobalCommitLog globalCommitLog() {
        return new GlobalCommitLog(maxTransactions);
    }

    @Bean
    public TransactionManager transactionManager(GlobalCommitLog cLog) {
        return new TransactionManager(cLog);
    }

    @Bean
    public CatalogManager catalogManager() {
        return new CatalogManager();
    }
}
