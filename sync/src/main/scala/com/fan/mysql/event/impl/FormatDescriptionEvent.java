package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;

public class FormatDescriptionEvent extends AbstractEvent {

    private int binlogVersion;
    private String serverVersion;
    private long createTime;
    private int commonHeaderLen;
    private short[] postHeaderLen;

    public FormatDescriptionEvent(EventHeader header) {
        super(header);
    }

    public FormatDescriptionEvent(int binlogChecksum) {
        super(new EventHeaderImpl());
        header.setChecksumAlg(binlogChecksum);
    }

    public int getBinlogVersion() {
        return binlogVersion;
    }

    public void setBinlogVersion(int binlogVersion) {
        this.binlogVersion = binlogVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public int getCommonHeaderLen() {
        return commonHeaderLen;
    }

    public void setCommonHeaderLen(int commonHeaderLen) {
        this.commonHeaderLen = commonHeaderLen;
    }

    public short[] getPostHeaderLen() {
        return postHeaderLen;
    }

    public void setPostHeaderLen(short[] postHeaderLen) {
        this.postHeaderLen = postHeaderLen;
    }

}
