package com.dingcloud.dts.binlog.event.impl;

import com.dingcloud.dts.binlog.event.AbstractEvent;
import com.dingcloud.dts.binlog.event.EventHeader;

public class DefaultEvent extends AbstractEvent {

	public DefaultEvent(EventHeader header) {
		super(header);
	}

}
