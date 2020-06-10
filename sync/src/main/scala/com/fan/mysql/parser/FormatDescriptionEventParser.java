package com.fan.mysql.parser;


import com.fan.mysql.binlog.BinlogEventParser;
import com.fan.mysql.dbsync.BinlogContext;
import com.fan.mysql.dbsync.LogBuffer;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.event.impl.FormatDescriptionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * ---------------------------------------------
 * 2                binlog-version
 * string[50]       mysql-server version
 * 4                create timestamp
 * 1                event header length
 * string[p]        event type header lengths
 * ---------------------------------------------
 * */
public class FormatDescriptionEventParser implements BinlogEventParser {

	private static final Logger logger = LoggerFactory.getLogger(FormatDescriptionEventParser.class);

	public static final int LOG_EVENT_HEADER_LEN = 19;

	public static final int BINLOG_VER_OFFSET = 0;
	public static final int SERVER_VER_OFFSET = 2;
	public static final int SERVER_VER_LEN = 50;
	public static final int CREATE_TIMESTAMP_OFFSET = SERVER_VER_OFFSET + SERVER_VER_LEN;
	public static final int CREATE_TIMESTAMP_LEN = 4;
	public static final int EVENT_HEADER_LEN_OFFSET = (CREATE_TIMESTAMP_OFFSET + CREATE_TIMESTAMP_LEN);

	public static final int[] checksumVersionSplit = { 5, 6, 1 };
	public static final long checksumVersionProduct = (checksumVersionSplit[0] * 256 + checksumVersionSplit[1]) * 256
			+ checksumVersionSplit[2];

	public FormatDescriptionEventParser() {
	}

	public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
		FormatDescriptionEvent event = new FormatDescriptionEvent(header);
		final int eventPos = buffer.position();
		// read version
		buffer.position(eventPos);
		int binlogVersion = buffer.getUint16();
		String serverVersion = buffer.getFixString(SERVER_VER_LEN);
		// read create time
		buffer.position(eventPos - LOG_EVENT_HEADER_LEN + CREATE_TIMESTAMP_OFFSET);
		long createTime = buffer.getUint32();
		// read event header length
		buffer.position(eventPos + EVENT_HEADER_LEN_OFFSET);
		int eventHeaderLen = buffer.getUint8();
		if (eventHeaderLen < LOG_EVENT_HEADER_LEN) {
			logger.error("common header length must >=" + LOG_EVENT_HEADER_LEN);
		}
		int numberOfEventTypes = buffer.limit() - (eventPos + EVENT_HEADER_LEN_OFFSET + 1);
		short[] postHeaderLen = new short[numberOfEventTypes];
		for (int i = 0; i < numberOfEventTypes; i++) {
			postHeaderLen[i] = (short) buffer.getUint8();
		}
		event.setBinlogVersion(binlogVersion);
		event.setServerVersion(serverVersion);
		event.setCreateTime(createTime);
		event.setCommonHeaderLen(eventHeaderLen);
		event.setPostHeaderLen(postHeaderLen);
		context.setFormatDescription(event);
		return event;
	}

	public static long versionProduct(int[] versionSplit) {
		return ((versionSplit[0] * 256 + versionSplit[1]) * 256 + versionSplit[2]);
	}

	public static void doServerVersionSplit(String serverVersion, int[] versionSplit) {
		String[] split = serverVersion.split("\\.");
		if (split.length < 3) {
			versionSplit[0] = 0;
			versionSplit[1] = 0;
			versionSplit[2] = 0;
		} else {
			int j = 0;
			for (int i = 0; i <= 2; i++) {
				String str = split[i];
				for (j = 0; j < str.length(); j++) {
					if (!Character.isDigit(str.charAt(j))) {
						break;
					}
				}
				if (j > 0) {
					versionSplit[i] = Integer.valueOf(str.substring(0, j), 10);
				} else {
					versionSplit[0] = 0;
					versionSplit[1] = 0;
					versionSplit[2] = 0;
				}
			}
		}
	}

}
