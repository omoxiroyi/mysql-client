package com.dingcloud.dts.binlog.parser;

import com.dingcloud.dts.binlog.BinlogEventParser;
import com.dingcloud.dts.binlog.event.BinlogEvent;
import com.dingcloud.dts.binlog.event.EventHeader;
import com.dingcloud.dts.binlog.event.impl.XidEvent;
import com.dingcloud.dts.binlog.mysql.dbsync.BinlogContext;
import com.dingcloud.dts.binlog.mysql.dbsync.LogBuffer;

public class XidEventParser implements BinlogEventParser {

	public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
		long xid = buffer.getLong64();
		XidEvent event = new XidEvent(header);
		event.setXid(xid);
		context.getTableMapEvents().clear();
		return event;
	}

}
