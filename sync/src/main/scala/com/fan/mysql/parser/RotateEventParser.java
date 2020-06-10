package com.dingcloud.dts.binlog.parser;

import com.dingcloud.dts.binlog.BinlogEventParser;
import com.dingcloud.dts.binlog.event.BinlogEvent;
import com.dingcloud.dts.binlog.event.EventHeader;
import com.dingcloud.dts.binlog.event.impl.RotateEvent;
import com.dingcloud.dts.binlog.mysql.dbsync.BinlogContext;
import com.dingcloud.dts.binlog.mysql.dbsync.LogBuffer;
import com.dingcloud.dts.binlog.util.ByteUtil;

public class RotateEventParser implements BinlogEventParser {

	public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
		long position = buffer.getLong64();
		int fileLen = buffer.limit() - buffer.position();
		String file = buffer.getFixString(fileLen);
		RotateEvent event = new RotateEvent(header);
		event.setBinlogFile(file);
		event.setBinlogPosition(position);
		context.setBinlogFileName(file);
		return event;
	}

	public static byte[] getBody(String fileName, long position) {
		byte[] body = new byte[fileName.length() + 8];
		ByteUtil.int8store(body, 0, position);
		byte[] b = fileName.getBytes();
		System.arraycopy(b, 0, body, 8, b.length);
		return body;
	}

}
