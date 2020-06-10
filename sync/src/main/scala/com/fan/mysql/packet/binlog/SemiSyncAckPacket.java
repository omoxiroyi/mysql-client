package com.dingcloud.dts.binlog.driver.packet;

import com.dingcloud.dts.binlog.driver.util.MySQLPacketBuffer;

public class SemiSyncAckPacket extends MySQLPacket {

	public static final byte magicNum = (byte) 0xef;

	private static final int REPLY_MAGIC_NUM_OFFSET = 0;

	private static final int REPLY_MAGIC_NUM_LEN = 1;

	private static final int REPLY_BINLOG_POS_LEN = 8;

	private static final int REPLY_BINLOG_POS_OFFSET = (REPLY_MAGIC_NUM_OFFSET + REPLY_MAGIC_NUM_LEN);

	private static final int REPLY_BINLOG_NAME_OFFSET = (REPLY_BINLOG_POS_OFFSET + REPLY_BINLOG_POS_LEN);

	private String fileName;
	private long pos;

	public SemiSyncAckPacket(String fileName, long pos) {
		this.fileName = fileName;
		this.pos = pos;
	}

	@Override
	public void write2Buffer(MySQLPacketBuffer buffer) {
		buffer.write(magicNum);
		buffer.writeLong(pos);
		buffer.writeStringNoNull(fileName);
	}

	@Override
	public String getPacketInfo() {
		return "MySQL Semi Sync Ack Packet";
	}

	@Override
	public int calcPacketSize() {
		int size = super.calcPacketSize();
		size += fileName.length();
		size += REPLY_BINLOG_NAME_OFFSET;
		return size;
	}

}
