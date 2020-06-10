package com.dingcloud.dts.binlog.event.impl;

import com.dingcloud.dts.binlog.event.AbstractEvent;

public class EOFLogEvent extends AbstractEvent {

	private final static EOFLogEvent _instance = new EOFLogEvent();

	protected EOFLogEvent() {
		super(null);
	}

	public static EOFLogEvent getInstance() {
		return _instance;
	}

}
