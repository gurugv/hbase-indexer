package com.ngdata.hbaseindexer.util.zookeeper;

public class LeaderElectionSetupException extends Exception {
    public LeaderElectionSetupException(String message, Throwable cause) {
        super(message, cause);
    }
}