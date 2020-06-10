package com.dingcloud.dts.binlog.position;

public abstract class LogPosition {

	public static LogPosition build(String posStr) {
		LogPosition logPos;
		if (posStr.contains(".")) {
			// file position
			String[] split = posStr.split(":");
			logPos = new BinlogFilePosition(split[0], Long.parseLong(split[1]));
		} else {
			// gtid position
			logPos = new GtidSetPosition(posStr);
		}
		return logPos;
	}

}
