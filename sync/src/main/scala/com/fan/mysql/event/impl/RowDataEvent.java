package com.dingcloud.dts.binlog.event.impl;

import java.util.ArrayList;
import java.util.List;

import com.dingcloud.dts.binlog.event.AbstractEvent;
import com.dingcloud.dts.binlog.event.EventHeader;

public class RowDataEvent extends AbstractEvent {

	private long tableId;
	private List<RowData> rows;
	private int flags;

	public RowDataEvent(EventHeader header, long tableId) {
		super(header);
		this.tableId = tableId;
		this.rows = new ArrayList<RowData>(2);
	}

	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public long getTableId() {
		return tableId;
	}

	public List<RowData> getRows() {
		return rows;
	}

}
