package com.oltpbenchmark.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class LoaderThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderThread.class);

    private final BenchmarkModule benchmarkModule;

    public LoaderThread(BenchmarkModule benchmarkModule) {
        this.benchmarkModule = benchmarkModule;
    }

    @Override
    public final void run() {
        beforeLoad();
        try (Connection conn = benchmarkModule.makeConnection()) {
            load(conn);
        } catch (SQLException ex) {
            SQLException next_ex = ex.getNextException();
            String msg = String.format("Unexpected error when loading %s database", benchmarkModule.getBenchmarkName().toUpperCase());
            LOG.error(msg, next_ex);
            throw new RuntimeException(ex);
        } finally {
            benchmarkModule.returnConnection();
            afterLoad();
        }
    }

    public abstract void load(Connection conn) throws SQLException;

    public void beforeLoad() {
    }

    public void afterLoad() {
    }

}
