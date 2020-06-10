package com.fan.mysql.parser;


import com.fan.mysql.binlog.BinlogEventFilter;
import com.fan.mysql.binlog.BinlogEventParser;

public abstract class FilterableParser implements BinlogEventParser {

	protected BinlogEventFilter filter;

	public void setFilter(BinlogEventFilter filter) {
		this.filter = filter;
	}
}
