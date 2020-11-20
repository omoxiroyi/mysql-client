package com.fan.mysql.event.impl;


import com.fan.mysql.event.EventHeader;
import com.fan.mysql.util.ByteUtil;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class EventHeaderImpl implements EventHeader {

    private long timestamp;
    private int eventType;
    private long serverId;
    private long eventLength;
    private long nextPosition;
    private int flags;

    private String binlogFileName;
    private long timestampOfReceipt;
    private int checksumAlg;

    public byte[] getByteArray() {
        byte[] buf = new byte[19];
        ByteUtil.int4store(buf, 0, timestamp);
        ByteUtil.int1store(buf, 4, eventType);
        ByteUtil.int4store(buf, 5, serverId);
        ByteUtil.int4store(buf, 9, eventLength);
        ByteUtil.int4store(buf, 13, nextPosition);
        ByteUtil.int2store(buf, 17, flags);
        return buf;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public int getChecksumAlg() {
        return checksumAlg;
    }

    public long getEventLength() {
        return eventLength;
    }

    public int getEventType() {
        return eventType;
    }

    public int getFlags() {
        return flags;
    }

    /**
     *
     */
    public int getHeaderLength() {
        return 19;
    }

    public long getNextPosition() {
        return nextPosition;
    }

    public long getPosition() {
        return this.nextPosition - this.eventLength;
    }

    public long getServerId() {
        return serverId;
    }

    /**
     *
     */
    public long getTimestamp() {
        return timestamp;
    }

    public long getTimestampOfReceipt() {
        return timestampOfReceipt;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    public void setChecksumAlg(int checksumAlg) {
        this.checksumAlg = checksumAlg;
    }

    public void setEventLength(long eventLength) {
        this.eventLength = eventLength;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setNextPosition(long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setTimestampOfReceipt(long timestampOfReceipt) {
        this.timestampOfReceipt = timestampOfReceipt;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("timestamp", timestamp).append("eventType", eventType)
                .append("serverId", serverId).append("eventLength", eventLength).append("nextPosition", nextPosition)
                .append("flags", flags).append("binlogFileName", binlogFileName)
                .append("timestampOfReceipt", timestampOfReceipt).toString();
    }
}
