package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;

import java.util.ArrayList;
import java.util.List;

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
