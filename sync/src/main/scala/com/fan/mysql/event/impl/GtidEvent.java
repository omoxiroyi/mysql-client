package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;

public class GtidEvent extends AbstractEvent {

    private byte[] sourceId;
    private long transactionId;
    private int logicalTimestamp;
    private long lastCommitted;
    private long sequenceNumber;

    public GtidEvent(EventHeader header) {
        super(header);
    }

    public byte[] getSourceId() {
        return sourceId;
    }

    public void setSourceId(byte[] sourceId) {
        this.sourceId = sourceId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public int getLogicalTimestamp() {
        return logicalTimestamp;
    }

    public void setLogicalTimestamp(int logicalTimestamp) {
        this.logicalTimestamp = logicalTimestamp;
    }

    public long getLastCommitted() {
        return lastCommitted;
    }

    public void setLastCommitted(long lastCommitted) {
        this.lastCommitted = lastCommitted;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String parseServerId() {
        StringBuilder sb = new StringBuilder();
        if (sourceId == null || sourceId.length <= 0) {
            return null;
        }
        for (int i = 0; i < sourceId.length; i++) {
            int v = sourceId[i] & 0xff;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                sb.append(0);
            }
            sb.append(hv);
            if (i == 3 | i == 5 | i == 7 | i == 9) {
                sb.append("-");
            }
        }
        return sb.toString();
    }

}
