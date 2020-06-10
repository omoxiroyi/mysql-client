package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;

public class EOFLogEvent extends AbstractEvent {

	private final static EOFLogEvent _instance = new EOFLogEvent();

	protected EOFLogEvent() {
		super(null);
	}

	public static EOFLogEvent getInstance() {
		return _instance;
	}

}
