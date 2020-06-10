package com.dingcloud.dts.binlog.parser;

import com.dingcloud.dts.binlog.BinlogEventParser;
import com.dingcloud.dts.binlog.event.BinlogEvent;
import com.dingcloud.dts.binlog.event.EventHeader;
import com.dingcloud.dts.binlog.event.impl.GtidEvent;
import com.dingcloud.dts.binlog.mysql.dbsync.BinlogContext;
import com.dingcloud.dts.binlog.mysql.dbsync.LogBuffer;

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
