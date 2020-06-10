package com.fan.mysql.column;

import java.io.Serializable;

public class EventColumn {

	private Serializable columnValue;
	private boolean isNull = false;

	public EventColumn() {
	}

	public Serializable getColumnValue() {
		return columnValue;
	}

	public void setColumnValue(Serializable columnValue) {
		this.columnValue = columnValue;
	}

	public boolean isNull() {
		return isNull;
	}

	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}

}
