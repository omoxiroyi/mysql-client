package com.fan.mysql.dbsync;


import com.fan.mysql.binlog.BinlogEventFilter;
import com.fan.mysql.binlog.BinlogEventParser;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.impl.DefaultEvent;
import com.fan.mysql.event.impl.EventHeaderImpl;
import com.fan.mysql.exception.BinlogParseException;
import com.fan.mysql.packet.binlog.SemiSyncAckPacket;
import com.fan.mysql.parser.*;
import com.fan.mysql.util.MySQLConstants;

import java.io.IOException;

public class BinlogDecoder {

    /**
     * Packet header sizes
     */
    public static final int NET_HEADER_SIZE = 4;

    public static final int LOG_EVENT_HEADER_LEN = 19;

    private final BinlogEventParser[] parsers = new BinlogEventParser[128];

    private BinlogEventFilter filter;

    public BinlogDecoder() {
    }

    public BinlogEvent decode(LogBuffer buffer, BinlogContext context) throws IOException {
        BinlogEvent event = null;
        // parse semi-sync header
        parseSemiSyncHeader(buffer, context);
        int start = buffer.position;
        int limit = buffer.limit;
        if (limit >= LOG_EVENT_HEADER_LEN) {
            // parse header
            EventHeaderImpl header = new EventHeaderImpl();
            parseHeader(header, buffer, context);
            context.setBinlogPosition(header.getNextPosition());
            if (limit >= header.getEventLength()) {
                // parse body
                BinlogEventParser parser = getEventParser(header.getEventType());
                if (parser != null) {
                    event = parser.parse(buffer, header, context);
                } else {
                    event = new DefaultEvent(header);
                }
                // set origin data
                buffer.limit(limit);
                event.setOriginData(buffer.getData(start, limit - start));
                return event;
            }
        }
        return event;
    }

    public void setFilter(BinlogEventFilter filter) {
        this.filter = filter;
    }

    private void parseSemiSyncHeader(LogBuffer buffer, BinlogContext context) {
        if (!context.isSemiSync()) {
            return;
        }
        int magicNum = buffer.getInt8();
        int needReply = buffer.getInt8();
//		buffer.forward(2);
        if (magicNum != SemiSyncAckPacket.magicNum) {
            throw new BinlogParseException("Magic number of semi-sync header is wrong!");
        }
        context.setNeedReply(needReply == 1);
    }

    private void parseHeader(EventHeaderImpl header, LogBuffer buffer, BinlogContext context) {
        int headerPos = buffer.position();
        header.setTimestamp(buffer.getUint32());
        header.setEventType(buffer.getUint8());
        header.setServerId(buffer.getUint32());
        header.setEventLength(buffer.getUint32());
        header.setNextPosition(buffer.getUint32());
        header.setFlags(buffer.getUint16());
        header.setBinlogFileName(context.getBinlogFileName());
        header.setTimestampOfReceipt(System.currentTimeMillis());
        int checksumAlg;
        if (header.getEventType() == MySQLConstants.FORMAT_DESCRIPTION_EVENT) {
            // handle binlog checksum
            buffer.position(headerPos + LOG_EVENT_HEADER_LEN + FormatDescriptionEventParser.EVENT_HEADER_LEN_OFFSET);
            int commonHeaderLen = buffer.getUint8();
            buffer.position(headerPos + commonHeaderLen + FormatDescriptionEventParser.SERVER_VER_OFFSET);
            String serverVersion = buffer.getFixString(FormatDescriptionEventParser.SERVER_VER_LEN);
            int[] versionSplit = new int[]{0, 0, 0};
            FormatDescriptionEventParser.doServerVersionSplit(serverVersion, versionSplit);
            checksumAlg = MySQLConstants.BINLOG_CHECKSUM_ALG_UNDEF;
            if (FormatDescriptionEventParser
                    .versionProduct(versionSplit) >= FormatDescriptionEventParser.checksumVersionProduct) {
                buffer.position(headerPos + (int) (header.getEventLength() - MySQLConstants.BINLOG_CHECKSUM_LEN
                        - MySQLConstants.BINLOG_CHECKSUM_ALG_DESC_LEN));
                checksumAlg = buffer.getUint8();
            }
        } else if (header.getEventType() == MySQLConstants.START_EVENT_V3) {
            // all following START events in the current file are without
            // checksum
            checksumAlg = MySQLConstants.BINLOG_CHECKSUM_ALG_OFF;
            context.getFormatDescription().getEventHeader().setChecksumAlg(checksumAlg);
        } else {
            checksumAlg = context.getFormatDescription().getEventHeader().getChecksumAlg();
        }
        header.setChecksumAlg(checksumAlg);
        buffer.position(headerPos + LOG_EVENT_HEADER_LEN);
        if (checksumAlg != MySQLConstants.BINLOG_CHECKSUM_ALG_UNDEF
                && (header.getEventType() == MySQLConstants.FORMAT_DESCRIPTION_EVENT
                || checksumAlg != MySQLConstants.BINLOG_CHECKSUM_ALG_OFF)) {
            buffer.limit(buffer.limit() - MySQLConstants.BINLOG_CHECKSUM_LEN);
        }
    }

    private BinlogEventParser getEventParser(int type) {
        BinlogEventParser parser = parsers[type];
        if (parser != null) {
            return parser;
        }
        switch (type) {
            case MySQLConstants.FORMAT_DESCRIPTION_EVENT: {
                parser = new FormatDescriptionEventParser();
                break;
            }
            case MySQLConstants.ROTATE_EVENT: {
                parser = new RotateEventParser();
                break;
            }
            case MySQLConstants.QUERY_EVENT: {
                parser = new QueryEventParser();
                break;
            }
            case MySQLConstants.TABLE_MAP_EVENT: {
                parser = new TableMapEventParser();
                TableMapEventParser tableMapParser = (TableMapEventParser) parser;
                tableMapParser.setFilter(filter);
                break;
            }
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V1:
            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V1:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V1: {
                parser = new RowDataEventParser();
                break;
            }
            case MySQLConstants.XID_EVENT: {
                parser = new XidEventParser();
                break;
            }
            case MySQLConstants.GTID_LOG_EVENT: {
                parser = new GtidEventParser();
                break;
            }
            case MySQLConstants.PREVIOUS_GTIDS_LOG_EVENT: {
                parser = new PreviousGtidsParser();
                break;
            }
            default:
                return null;
        }
        parsers[type] = parser;
        return parser;
    }

}
