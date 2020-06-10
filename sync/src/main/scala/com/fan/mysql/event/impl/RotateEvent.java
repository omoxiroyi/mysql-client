package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;

public class RotateEvent extends AbstractEvent {

	private long binlogPosition;
	private String binlogFile;

	public RotateEvent(EventHeader header) {
		super(header);
	}
	
	public long getBinlogPosition() {
		return binlogPosition;
	}

	public String getBinlogFile() {
		return binlogFile;
	}

	public void setBinlogPosition(long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}

	public void setBinlogFile(String binlogFile) {
		this.binlogFile = binlogFile;
	}

}
