package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.parser.QueryEventParser;

public class QueryEvent extends AbstractEvent {

	private long threadId;
	private long execTime;
	private int databaseLength;
	private int errorCode;
	private int statusVariablesLength;
	private QueryEventParser.StatusVariable statusVariables;
	private String databaseName;
	private String query;

	public QueryEvent(EventHeader header) {
		super(header);
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public long getExecTime() {
		return execTime;
	}

	public void setExecTime(long execTime) {
		this.execTime = execTime;
	}

	public int getDatabaseLength() {
		return databaseLength;
	}

	public void setDatabaseLength(int databaseLength) {
		this.databaseLength = databaseLength;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public int getStatusVariablesLength() {
		return statusVariablesLength;
	}

	public void setStatusVariablesLength(int statusVariablesLength) {
		this.statusVariablesLength = statusVariablesLength;
	}

	public QueryEventParser.StatusVariable getStatusVariables() {
		return statusVariables;
	}

	public void setStatusVariables(QueryEventParser.StatusVariable statusVariables) {
		this.statusVariables = statusVariables;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}
