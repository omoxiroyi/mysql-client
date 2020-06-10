package com.fan.mysql.event.impl;


import com.fan.mysql.event.AbstractEvent;
import com.fan.mysql.event.EventHeader;

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
