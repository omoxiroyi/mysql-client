package com.fan.mysql.async.binlog.position;

@SuppressWarnings("unused")
public class BinlogFilePosition extends LogPosition {

    private final String fileName;
    private final long position;

    public BinlogFilePosition(String fileName, final long position) {
        this.fileName = fileName;
        this.position = position;
    }

    public final String getFileName() {
        return fileName;
    }

    public final long getPosition() {
        return position;
    }

    public int compareTo(LogPosition pos) {
        BinlogFilePosition filePos;
        if (pos instanceof BinlogFilePosition) {
            filePos = (BinlogFilePosition) pos;
        } else {
            return -1;
        }
        final int val = fileName.compareTo(filePos.fileName);
        if (val == 0) {
            return (int) (position - filePos.position);
        }
        return val;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof BinlogFilePosition) {
            BinlogFilePosition pos = ((BinlogFilePosition) obj);
            return fileName.equals(pos.fileName) && (this.position == pos.position);
        }
        return false;
    }

    public String toString() {
        return fileName + ':' + position;
    }

}
