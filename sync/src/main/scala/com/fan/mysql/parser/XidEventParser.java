package com.fan.mysql.parser;


import com.fan.mysql.binlog.BinlogEventParser;
import com.fan.mysql.dbsync.BinlogContext;
import com.fan.mysql.dbsync.LogBuffer;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.event.impl.XidEvent;

public class XidEventParser implements BinlogEventParser {

	public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
		long xid = buffer.getLong64();
		XidEvent event = new XidEvent(header);
		event.setXid(xid);
		context.getTableMapEvents().clear();
		return event;
	}

}
