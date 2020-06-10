package com.fan.mysql.parser;


import com.fan.mysql.binlog.BinlogEventParser;
import com.fan.mysql.column.EventColumn;
import com.fan.mysql.dbsync.BinlogContext;
import com.fan.mysql.dbsync.LogBuffer;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.event.impl.DefaultEvent;
import com.fan.mysql.event.impl.RowData;
import com.fan.mysql.event.impl.RowDataEvent;
import com.fan.mysql.event.impl.TableMapEvent;
import com.fan.mysql.util.JsonConversion;
import com.fan.mysql.util.MySQLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.Calendar;
import java.util.TimeZone;

/*
 *
 * header:
 *   if post_header_len == 6 {
 * 4                    table id
 *   } else {
 * 6                    table id
 *   }
 * 2                    flags
 *   if version == 2 {
 * 2                    extra-data-length
 * string.var_len       extra-data
 *   }
 *
 * body:
 * lenenc_int           number of columns
 * string.var_len       columns-present-bitmap1, length: (num of columns+7)/8
 *   if UPDATE_ROWS_EVENTv1 or v2 {
 * string.var_len       columns-present-bitmap2, length: (num of columns+7)/8
 *   }
 *
 * rows:
 * string.var_len       nul-bitmap, length (bits set in 'columns-present-bitmap1'+7)/8
 * string.var_len       value of each field as defined in table-map
 *   if UPDATE_ROWS_EVENTv1 or v2 {
 * string.var_len       nul-bitmap, length (bits set in 'columns-present-bitmap2'+7)/8
 * string.var_len       value of each field as defined in table-map
 *   }
 *   ... repeat rows until event-end
 *
 * */
@SuppressWarnings("unused")
public class RowDataEventParser implements BinlogEventParser {

    private static final Logger logger = LoggerFactory.getLogger(RowDataEventParser.class);

    public static final int RW_V_EXTRAINFO_TAG = 0;
    public static final int EXTRA_ROW_INFO_LEN_OFFSET = 0;
    public static final int EXTRA_ROW_INFO_FORMAT_OFFSET = 1;
    public static final int EXTRA_ROW_INFO_HDR_BYTES = 2;
    public static final int EXTRA_ROW_INFO_MAX_PAYLOAD = (255 - EXTRA_ROW_INFO_HDR_BYTES);

    public static final long DATETIMEF_INT_OFS = 0x8000000000L;
    public static final long TIMEF_INT_OFS = 0x800000L;
    public static final long TIMEF_OFS = 0x800000000000L;

    public static final int STMT_END_F = 1;
    public static final int NO_FOREIGN_KEY_CHECKS_F = (1 << 1);
    public static final int RELAXED_UNIQUE_CHECKS_F = (1 << 2);
    public static final int COMPLETE_ROWS_F = (1 << 3);

    private int columnLen;
    private BitSet nullBits;
    private int nullBitIndex;

    public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
        final int eventPos = buffer.position();
        // read row event header
        int postHeaderLen = context.getFormatDescription().getPostHeaderLen()[header.getEventType() - 1];
        buffer.position(eventPos);
        // read table id
        long tableId;
        if (postHeaderLen == 6) {
            tableId = buffer.getUint32();
        } else {
            tableId = buffer.getUlong48();
        }
        // filter by table name
        TableMapEvent tableMap = context.getTableMapEvents().get(tableId);
        if (tableMap == null) {
            return new DefaultEvent(header);
        }
        RowDataEvent event = new RowDataEvent(header, tableId);
        // read flag
        int flags = buffer.getUint16();
        event.setFlags(flags);
        // read extra data
        int headerLen = 0;
        if (postHeaderLen == 10) {
            headerLen = buffer.getUint16();
            headerLen -= 2;
            int start = buffer.position();
            int end = start + headerLen;
            for (int i = start; i < end; ) {
                if (buffer.getUint8(i++) == RW_V_EXTRAINFO_TAG) {
                    buffer.position(i + EXTRA_ROW_INFO_LEN_OFFSET);
                    int checkLen = buffer.getUint8();
                    int val = checkLen - EXTRA_ROW_INFO_HDR_BYTES;
                    assert (buffer.getUint8() == val);
                    for (int j = 0; j < val; j++) {
                        assert (buffer.getUint8() == val);
                    }
                } else {
                    i = end;
                }
            }
        }
        // read row event body
        buffer.position(eventPos + postHeaderLen + headerLen);
        columnLen = (int) buffer.getPackedLong();
        BitSet columns = buffer.getBitmap(columnLen);
        BitSet changedColumns = null;
        if (header.getEventType() == MySQLConstants.UPDATE_ROWS_EVENT_V1
                || header.getEventType() == MySQLConstants.UPDATE_ROWS_EVENT) {
            changedColumns = buffer.getBitmap(columnLen);
        }
        nullBits = new BitSet(columnLen);
        // parse and convert to row event
        String timeZone = context.getTimeZone();
        while (nextOneRow(buffer, columns)) {
            RowData row = new RowData();
            if (header.getEventType() == MySQLConstants.WRITE_ROWS_EVENT
                    || header.getEventType() == MySQLConstants.WRITE_ROWS_EVENT_V1) {
                parseRow(buffer, row, tableMap, columns, true, timeZone);
            } else if (header.getEventType() == MySQLConstants.DELETE_ROWS_EVENT
                    || header.getEventType() == MySQLConstants.DELETE_ROWS_EVENT_V1) {
                parseRow(buffer, row, tableMap, columns, false, timeZone);
            } else if (header.getEventType() == MySQLConstants.UPDATE_ROWS_EVENT
                    || header.getEventType() == MySQLConstants.UPDATE_ROWS_EVENT_V1) {
                parseRow(buffer, row, tableMap, columns, false, timeZone);
                if (!nextOneRow(buffer, changedColumns)) {
                    break;
                }
                parseRow(buffer, row, tableMap, changedColumns, true, timeZone);
            }
            event.getRows().add(row);
        }
        // end of statement check:
        if ((flags & STMT_END_F) != 0) {
            context.getTableMapEvents().clear();
        }
        return event;
    }

    private void parseRow(LogBuffer buffer, RowData row, TableMapEvent tableMap, BitSet colBit, boolean isAfter,
                          String timeZone) {
        int currentColumn = 0;
        int columnCount = 0;
        // calculate real column count
        for (int i = 0; i < columnLen; i++) {
            if (!colBit.get(i)) {
                continue;
            } else {
                columnCount++;
            }
        }
        EventColumn[] rowData = new EventColumn[columnCount];
        // parse row value
        for (int i = 0; i < columnLen; i++) {
            if (!colBit.get(i)) {
                continue;
            }
            TableMapEvent.ColumnInfo cInfo = tableMap.getColumnInfo()[i];
            EventColumn column = new EventColumn();
            if (nullBits.get(nullBitIndex++)) {
                column.setNull(true);
            } else {
                Serializable value = fetchValue(buffer, cInfo.type, cInfo.meta, timeZone);
                column.setColumnValue(value);
            }
            rowData[currentColumn++] = column;
        }
        // set event keys and columns
        if (isAfter) {
            row.setAfterColumns(rowData);
            row.setAfterBit(colBit);

        } else {
            row.setBeforeColumns(rowData);
            row.setBeforeBit(colBit);
        }
    }

    private Serializable fetchValue(LogBuffer buffer, int type, int meta, String timeZone) {
        Serializable value;
        int len = 0;
        if (type == MySQLConstants.MYSQL_TYPE_STRING) {
            if (meta >= 256) {
                int byte0 = meta >> 8;
                int byte1 = meta & 0xff;
                if ((byte0 & 0x30) != 0x30) {
                    /* a long CHAR() field: see #37426 */
                    len = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                    type = byte0 | 0x30;
                } else {
                    switch (byte0) {
                        case MySQLConstants.MYSQL_TYPE_SET:
                        case MySQLConstants.MYSQL_TYPE_ENUM:
                        case MySQLConstants.MYSQL_TYPE_STRING:
                            type = byte0;
                            len = byte1;
                            break;
                        default:
                            throw new IllegalArgumentException(String
                                    .format("!! Don't know how to handle column type=%d meta=%d (%04X)", type, meta, meta));
                    }
                }
            } else {
                len = meta;
            }
        }
        switch (type) {
            case MySQLConstants.MYSQL_TYPE_TINY: {
                byte[] num = new byte[1];
                buffer.fillBytes(num, 0, 1);
                value = num;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_SHORT: {
                byte[] num = new byte[2];
                buffer.fillBytes(num, 0, 2);
                value = num;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_INT24: {
                byte[] num = new byte[3];
                buffer.fillBytes(num, 0, 3);
                value = num;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_LONG: {
                byte[] num = new byte[4];
                buffer.fillBytes(num, 0, 4);
                value = num;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_LONGLONG: {
                byte[] num = new byte[8];
                buffer.fillBytes(num, 0, 8);
                value = num;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_DECIMAL: {
                logger.warn("MYSQL_TYPE_DECIMAL : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
                value = null; /* unknown format */
                break;
            }
            case MySQLConstants.MYSQL_TYPE_NEWDECIMAL: {
                final int precision = meta >> 8;
                final int decimals = meta & 0xff;
                BigDecimal number = buffer.getDecimal(precision, decimals);
                value = number.toPlainString();
                break;
            }
            case MySQLConstants.MYSQL_TYPE_FLOAT: {
                value = buffer.getFloat32();
                break;
            }
            case MySQLConstants.MYSQL_TYPE_DOUBLE: {
                value = buffer.getDouble64();
                break;
            }
            case MySQLConstants.MYSQL_TYPE_BIT: {
                /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
                final int nbits = ((meta >> 8) * 8) + (meta & 0xff);
                len = (nbits + 7) / 8;
                if (nbits <= 1) {
                    len = 1;
                }
                byte[] bit = new byte[len];
                buffer.fillBytes(bit, 0, len);
                value = bit;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_TIMESTAMP: {
                final long i32 = buffer.getUint32();
                if (i32 == 0) {
                    value = "0000-00-00 00:00:00";
                } else {
                    String v = new Timestamp(i32 * 1000).toString();
                    value = v.substring(0, v.length() - 2);
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_TIMESTAMP2: {
                final long tv_sec = buffer.getBeUint32();
                int tv_usec = 0;
                switch (meta) {
                    case 1:
                    case 2:
                        tv_usec = buffer.getInt8() * 10000;
                        break;
                    case 3:
                    case 4:
                        tv_usec = buffer.getBeInt16() * 100;
                        break;
                    case 5:
                    case 6:
                        tv_usec = buffer.getBeInt24();
                        break;
                    default:
                        break;
                }

                String second = null;
                if (tv_sec == 0) {
                    second = "0000-00-00 00:00:00";
                } else {
                    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
                    cal.setTimeInMillis(tv_sec * 1000);

                    second = calendarToStr(cal);
                }

                if (meta >= 1) {
                    String microSecond = usecondsToStr(tv_usec, meta);
                    microSecond = microSecond.substring(0, meta);
                    value = second + '.' + microSecond;
                } else {
                    value = second;
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_DATETIME: {
                final long i64 = buffer.getLong64();
                if (i64 == 0) {
                    value = "0000-00-00 00:00:00";
                } else {
                    final int d = (int) (i64 / 1000000);
                    final int t = (int) (i64 % 1000000);
                    value = String.format("%04d-%02d-%02d %02d:%02d:%02d", d / 10000, (d % 10000) / 100, d % 100, t / 10000,
                            (t % 10000) / 100, t % 100);
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_DATETIME2: {
                long intpart = buffer.getBeUlong40() - DATETIMEF_INT_OFS;
                int frac = 0;
                switch (meta) {
                    case 1:
                    case 2:
                        frac = buffer.getInt8() * 10000;
                        break;
                    case 3:
                    case 4:
                        frac = buffer.getBeInt16() * 100;
                        break;
                    case 5:
                    case 6:
                        frac = buffer.getBeInt24();
                        break;
                    default:
                        break;
                }

                String second;
                if (intpart == 0) {
                    second = "0000-00-00 00:00:00";
                } else {
                    // 构造TimeStamp只处理到秒
                    long ymd = intpart >> 17;
                    long ym = ymd >> 5;
                    long hms = intpart % (1 << 17);
                    second = String.format("%04d-%02d-%02d %02d:%02d:%02d", (int) (ym / 13), (int) (ym % 13),
                            (int) (ymd % (1 << 5)), (int) (hms >> 12), (int) ((hms >> 6) % (1 << 6)),
                            (int) (hms % (1 << 6)));
                }

                if (meta >= 1) {
                    String microSecond = usecondsToStr(frac, meta);
                    microSecond = microSecond.substring(0, meta);
                    value = second + '.' + microSecond;
                } else {
                    value = second;
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_TIME: {
                final int i32 = buffer.getInt24();
                final int u32 = Math.abs(i32);
                if (i32 == 0) {
                    value = "00:00:00";
                } else {
                    value = String.format("%s%02d:%02d:%02d", (i32 >= 0) ? "" : "-", u32 / 10000, (u32 % 10000) / 100,
                            u32 % 100);
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_TIME2: {
                long intpart = 0;
                int frac = 0;
                long ltime = 0;
                switch (meta) {
                    case 1:
                    case 2:
                        intpart = buffer.getBeUint24() - TIMEF_INT_OFS;
                        frac = buffer.getUint8();
                        if (intpart < 0 && frac > 0) {
                            intpart++;
                            frac -= 0x100;
                        }
                        frac = frac * 10000;
                        ltime = intpart << 24;
                        break;
                    case 3:
                    case 4:
                        intpart = buffer.getBeUint24() - TIMEF_INT_OFS;
                        frac = buffer.getBeUint16();
                        if (intpart < 0 && frac > 0) {
                            intpart++;
                            frac -= 0x10000;
                        }
                        frac = frac * 100;
                        ltime = intpart << 24;
                        break;
                    case 5:
                    case 6:
                        intpart = buffer.getBeUlong48() - TIMEF_OFS;
                        ltime = intpart;
                        frac = (int) (intpart % (1L << 24));
                        break;
                    default:
                        intpart = buffer.getBeUint24() - TIMEF_INT_OFS;
                        ltime = intpart << 24;
                        break;
                }

                String second = null;
                if (intpart == 0) {
                    second = "00:00:00";
                } else {
                    long ultime = Math.abs(ltime);
                    intpart = ultime >> 24;
                    second = String.format("%s%02d:%02d:%02d", ltime >= 0 ? "" : "-", (int) ((intpart >> 12) % (1 << 10)),
                            (int) ((intpart >> 6) % (1 << 6)), (int) (intpart % (1 << 6)));
                }

                if (meta >= 1) {
                    String microSecond = usecondsToStr(Math.abs(frac), meta);
                    microSecond = microSecond.substring(0, meta);
                    value = second + '.' + microSecond;
                } else {
                    value = second;
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_NEWDATE: {
                logger.warn("MYSQL_TYPE_NEWDATE : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
                value = null; /* unknown format */
                break;
            }
            case MySQLConstants.MYSQL_TYPE_DATE: {
                final int i32 = buffer.getUint24();
                if (i32 == 0) {
                    value = "0000-00-00";
                } else {
                    value = String.format("%04d-%02d-%02d", i32 / (16 * 32), i32 / 32 % 16, i32 % 32);
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_YEAR: {
                final int i32 = buffer.getUint8();
                if (i32 == 0) {
                    value = "0000";
                } else {
                    value = String.valueOf((short) (i32 + 1900));
                }
                break;
            }
            case MySQLConstants.MYSQL_TYPE_ENUM: {
                final byte[] int32;
                switch (len) {
                    case 1:
                        int32 = new byte[1];
                        buffer.fillBytes(int32, 0, 1);
                        break;
                    case 2:
                        int32 = new byte[2];
                        buffer.fillBytes(int32, 0, 2);
                        break;
                    default:
                        throw new IllegalArgumentException("!! Unknown ENUM packlen = " + len);
                }
                value = int32;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_SET: {
                final int nbits = (meta & 0xFF) * 8;
                len = (nbits + 7) / 8;
                if (nbits <= 1) {
                    len = 1;
                }
                byte[] set = new byte[len];
                buffer.fillBytes(set, 0, len);
                value = set;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_TINY_BLOB: {
                logger.warn("MYSQL_TYPE_TINY_BLOB : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
            }
            case MySQLConstants.MYSQL_TYPE_MEDIUM_BLOB: {
                logger.warn("MYSQL_TYPE_MEDIUM_BLOB : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
            }
            case MySQLConstants.MYSQL_TYPE_LONG_BLOB: {
                logger.warn("MYSQL_TYPE_LONG_BLOB : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
            }
            case MySQLConstants.MYSQL_TYPE_BLOB: {
                byte[] binary;
                switch (meta) {
                    case 1: {
                        /* TINYBLOB/TINYTEXT */
                        final int len8 = buffer.getUint8();
                        binary = new byte[len8];
                        buffer.fillBytes(binary, 0, len8);
                        break;
                    }
                    case 2: {
                        /* BLOB/TEXT */
                        final int len16 = buffer.getUint16();
                        binary = new byte[len16];
                        buffer.fillBytes(binary, 0, len16);
                        break;
                    }
                    case 3: {
                        /* MEDIUMBLOB/MEDIUMTEXT */
                        final int len24 = buffer.getUint24();
                        binary = new byte[len24];
                        buffer.fillBytes(binary, 0, len24);
                        break;
                    }
                    case 4: {
                        /* LONGBLOB/LONGTEXT */
                        final int len32 = (int) buffer.getUint32();
                        binary = new byte[len32];
                        buffer.fillBytes(binary, 0, len32);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("!! Unknown BLOB packlen = " + meta);
                }
                value = binary;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_VARCHAR:
            case MySQLConstants.MYSQL_TYPE_VAR_STRING: {
                len = meta;
                if (len < 256) {
                    len = buffer.getUint8();
                } else {
                    len = buffer.getUint16();
                }
                byte[] binary = new byte[len];
                buffer.fillBytes(binary, 0, len);
                value = binary;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_STRING: {
                if (len < 256) {
                    len = buffer.getUint8();
                } else {
                    len = buffer.getUint16();
                }
                byte[] binary = new byte[len];
                buffer.fillBytes(binary, 0, len);
                value = binary;
                break;
            }
            case MySQLConstants.MYSQL_TYPE_JSON: {
                len = buffer.getUint16();
                buffer.forward(meta - 2);
                int position = buffer.position();
                JsonConversion.Json_Value jsonValue = JsonConversion.parse_value(buffer.getUint8(), buffer, len - 1);
                StringBuilder builder = new StringBuilder();
                jsonValue.toJsonString(builder);
                value = builder.toString();
                buffer.position(position + len);
                break;
            }
            case MySQLConstants.MYSQL_TYPE_GEOMETRY: {
                switch (meta) {
                    case 1:
                        len = buffer.getUint8();
                        break;
                    case 2:
                        len = buffer.getUint16();
                        break;
                    case 3:
                        len = buffer.getUint24();
                        break;
                    case 4:
                        len = (int) buffer.getUint32();
                        break;
                    default:
                        throw new IllegalArgumentException("!! Unknown MYSQL_TYPE_GEOMETRY packlen = " + meta);
                }
                /* fill binary */
                byte[] binary = new byte[len];
                buffer.fillBytes(binary, 0, len);
                value = binary;
                break;
            }
            default:
                logger.error(String.format("!! Don't know how to handle column type=%d meta=%d (%04X)", type, meta, meta));
                value = null;
        }
        return value;

    }

    private boolean nextOneRow(LogBuffer buffer, BitSet columns) {
        final boolean hasOneRow = buffer.hasRemaining();
        if (hasOneRow) {
            int column = 0;

            for (int i = 0; i < columnLen; i++)
                if (columns.get(i))
                    column++;

            nullBitIndex = 0;
            nullBits.clear();
            buffer.fillBitmap(nullBits, column);
        }
        return hasOneRow;
    }

    private String calendarToStr(Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
    }

    private String usecondsToStr(int frac, int meta) {
        String sec = String.valueOf(frac);
        if (meta > 6) {
            throw new IllegalArgumentException("unknow useconds meta : " + meta);
        }

        if (sec.length() < 6) {
            StringBuilder result = new StringBuilder(6);
            int len = 6 - sec.length();
            for (; len > 0; len--) {
                result.append('0');
            }
            result.append(sec);
            sec = result.toString();
        }

        return sec.substring(0, meta);
    }

}
