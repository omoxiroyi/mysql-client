package com.dingcloud.dts.binlog.event.impl;

import com.dingcloud.dts.binlog.event.AbstractEvent;
import com.dingcloud.dts.binlog.event.EventHeader;

public class PreviousGtidsEvent extends AbstractEvent {

	private String gtidSet;

	public PreviousGtidsEvent(EventHeader header) {
		super(header);
	}

	public String getGtidSet() {
		return gtidSet;
	}

	public void setGtidSet(String gtidSet) {
		this.gtidSet = gtidSet;
	}

}
