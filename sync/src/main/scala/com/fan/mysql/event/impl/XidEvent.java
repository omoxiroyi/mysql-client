package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;

public class XidEvent extends AbstractEvent {

	private long xid;

	public XidEvent(EventHeader header) {
		super(header);
	}

	public long getXid() {
		return xid;
	}

	public void setXid(long xid) {
		this.xid = xid;
	}

}
