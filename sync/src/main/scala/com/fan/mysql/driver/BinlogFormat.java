package com.fan.mysql.driver;

public enum BinlogFormat {

    STATEMENT("STATEMENT"), ROW("ROW"), MIXED("MIXED");

    public boolean isStatement() {
        return this == STATEMENT;
    }

    public boolean isRow() {
        return this == ROW;
    }

    public boolean isMixed() {
        return this == MIXED;
    }

    private final String value;

    private BinlogFormat(String value) {
        this.value = value;
    }

    public static BinlogFormat valuesOf(String value) {
        BinlogFormat[] formats = values();
        for (BinlogFormat format : formats) {
            if (format.value.equalsIgnoreCase(value)) {
                return format;
            }
        }
        return null;
    }
}