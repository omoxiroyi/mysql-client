package com.dingcloud.dts.binlog.parser;

import com.dingcloud.dts.binlog.BinlogEventFilter;
import com.dingcloud.dts.binlog.BinlogEventParser;

public abstract class FilterableParser implements BinlogEventParser {

	protected BinlogEventFilter filter;

	public void setFilter(BinlogEventFilter filter) {
		this.filter = filter;
	}
}
