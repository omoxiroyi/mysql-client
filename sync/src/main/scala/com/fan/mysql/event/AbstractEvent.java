package com.fan.mysql.event;

public abstract class AbstractEvent implements BinlogEvent {

    protected EventHeader header;
    protected byte[] originData;

    public AbstractEvent(EventHeader header) {
        this.header = header;
    }

    public EventHeader getEventHeader() {
        return header;
    }

    public byte[] getOriginData() {
        return originData;
    }

    public void setOriginData(byte[] originData) {
        this.originData = originData;
    }

}
