package com.fan.mysql.parser;


import com.fan.mysql.binlog.BinlogEventParser;
import com.fan.mysql.dbsync.BinlogContext;
import com.fan.mysql.dbsync.LogBuffer;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.event.impl.GtidEvent;

public class GtidEventParser implements BinlogEventParser {

    public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
        GtidEvent event = new GtidEvent(header);
        buffer.getInt8(); // commit flag, always true
        event.setSourceId(buffer.getData(16));
        event.setTransactionId(buffer.getLong64());
        if (buffer.hasRemaining()) {
            event.setLogicalTimestamp(buffer.getInt8());
            event.setLastCommitted(buffer.getLong64());
            event.setSequenceNumber(buffer.getLong64());
        }
        return event;
    }

}
