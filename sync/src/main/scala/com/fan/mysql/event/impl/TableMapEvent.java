package com.fan.mysql.event.impl;

import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;

import java.util.BitSet;

public class TableMapEvent extends AbstractEvent {

	private String dbname;
	private String tblname;
	private int columnCnt;
	private ColumnInfo[] columnInfo;
	private long tableId;
	private BitSet nullBits;

	public TableMapEvent(EventHeader header) {
		super(header);
	}

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getTblname() {
		return tblname;
	}

	public void setTblname(String tblname) {
		this.tblname = tblname;
	}

	public int getColumnCnt() {
		return columnCnt;
	}

	public void setColumnCnt(int columnCnt) {
		this.columnCnt = columnCnt;
	}

	public ColumnInfo[] getColumnInfo() {
		return columnInfo;
	}

	public void setColumnInfo(ColumnInfo[] columnInfo) {
		this.columnInfo = columnInfo;
	}

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public BitSet getNullBits() {
		return nullBits;
	}

	public void setNullBits(BitSet nullBits) {
		this.nullBits = nullBits;
	}

	public static final class ColumnInfo {
		public int type;
		public int meta;
	}

}
