package com.fan.mysql.parser;


import com.fan.mysql.dbsync.BinlogContext;
import com.fan.mysql.dbsync.LogBuffer;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.event.impl.DefaultEvent;
import com.fan.mysql.event.impl.TableMapEvent;
import com.fan.mysql.util.MySQLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;

public class TableMapEventParser extends FilterableParser {

    private static final Logger logger = LoggerFactory.getLogger(TableMapEventParser.class);

    public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
        TableMapEvent event = new TableMapEvent(header);
        final int eventPos = buffer.position();
        final int postHeaderLen = context.getFormatDescription().getPostHeaderLen()[header.getEventType() - 1];
        buffer.position(eventPos);
        long tableId;
        if (postHeaderLen == 6) {
            tableId = buffer.getUint32();
        } else {
            tableId = buffer.getUlong48();
        }
        // flags = buffer.getUint16();
        /* Read the variable part of the event */
        buffer.position(eventPos + postHeaderLen);
        String dbname = buffer.getString();
        buffer.forward(1); /* termination null */
        String tblname = buffer.getString();
        buffer.forward(1); /* termination null */
        event.setTableId(tableId);
        event.setDbname(dbname);
        event.setTblname(tblname);
        // filter by table name
        if (filter != null && !filter.accepts(event)) {
            return new DefaultEvent(header);
        }
        // Read column information from buffer
        int columnCnt = (int) buffer.getPackedLong();
        TableMapEvent.ColumnInfo[] columnInfo = new TableMapEvent.ColumnInfo[columnCnt];
        for (int i = 0; i < columnCnt; i++) {
            TableMapEvent.ColumnInfo info = new TableMapEvent.ColumnInfo();
            info.type = buffer.getUint8();
            columnInfo[i] = info;
        }
        BitSet nullBits = null;
        if (buffer.position() < buffer.limit()) {
            final int fieldSize = (int) buffer.getPackedLong();
            decodeFields(buffer, fieldSize, columnCnt, columnInfo);
            nullBits = buffer.getBitmap(columnCnt);
        }
        // set table map event

        event.setColumnCnt(columnCnt);
        event.setColumnInfo(columnInfo);
        event.setNullBits(nullBits);
        // put to context table map
        context.getTableMapEvents().put(tableId, event);
        return event;
    }

    private final void decodeFields(LogBuffer buffer, final int len, int columnCnt, TableMapEvent.ColumnInfo[] columnInfo) {
        final int limit = buffer.limit();

        buffer.limit(len + buffer.position());
        for (int i = 0; i < columnCnt; i++) {
            TableMapEvent.ColumnInfo info = columnInfo[i];

            switch (info.type) {
                case MySQLConstants.MYSQL_TYPE_TINY_BLOB:
                case MySQLConstants.MYSQL_TYPE_BLOB:
                case MySQLConstants.MYSQL_TYPE_MEDIUM_BLOB:
                case MySQLConstants.MYSQL_TYPE_LONG_BLOB:
                case MySQLConstants.MYSQL_TYPE_DOUBLE:
                case MySQLConstants.MYSQL_TYPE_FLOAT:
                case MySQLConstants.MYSQL_TYPE_GEOMETRY:
                case MySQLConstants.MYSQL_TYPE_JSON:
                    /*
                     * These types store a single byte.
                     */
                    info.meta = buffer.getUint8();
                    break;
                case MySQLConstants.MYSQL_TYPE_SET:
                case MySQLConstants.MYSQL_TYPE_ENUM:
                    /*
                     * log_event.h : MYSQL_TYPE_SET & MYSQL_TYPE_ENUM : This enumeration value is
                     * only used internally and cannot exist in a binlog.
                     */
                    logger.warn("This enumeration value is only used internally " + "and cannot exist in a binlog: type="
                            + info.type);
                    break;
                case MySQLConstants.MYSQL_TYPE_STRING:
                case MySQLConstants.MYSQL_TYPE_NEWDECIMAL: {
                    /*
                     * log_event.h : The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253). The
                     * second byte is the field size, i.e., the number of bytes in the
                     * representation of size of the string: 3 or 4.
                     */
                    int x = (buffer.getUint8() << 8); // real_type
                    x += buffer.getUint8(); // pack or field length
                    info.meta = x;
                    break;
                }
                case MySQLConstants.MYSQL_TYPE_BIT:
                case MySQLConstants.MYSQL_TYPE_VARCHAR:
                    /*
                     * These types store two bytes.
                     */
                    info.meta = buffer.getUint16();
                    break;
                // precision
// decimals
                case MySQLConstants.MYSQL_TYPE_TIME2:
                case MySQLConstants.MYSQL_TYPE_DATETIME2:
                case MySQLConstants.MYSQL_TYPE_TIMESTAMP2: {
                    info.meta = buffer.getUint8();
                    break;
                }
                default:
                    info.meta = 0;
                    break;
            }
        }
        buffer.limit(limit);
    }

}
