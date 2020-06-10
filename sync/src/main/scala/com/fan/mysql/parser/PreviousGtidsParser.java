package com.fan.mysql.parser;


import com.fan.mysql.binlog.BinlogEventParser;
import com.fan.mysql.dbsync.BinlogContext;
import com.fan.mysql.dbsync.LogBuffer;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.event.impl.PreviousGtidsEvent;
import com.fan.mysql.position.GtidSetPosition;
import com.fan.mysql.util.ByteUtil;

import java.math.BigInteger;
import java.util.Collection;

@SuppressWarnings("unused")
public class PreviousGtidsParser implements BinlogEventParser {

    @Override
    public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
        PreviousGtidsEvent event = new PreviousGtidsEvent(header);
        StringBuilder sb = new StringBuilder();
        long sidNumberCount = buffer.getLong64();
        for (int i = 0; i < sidNumberCount; i++) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            byte[] sourcId = buffer.getData(16);
            String uuidStr = parseServerId(sourcId);
            sb.append(uuidStr);
            long internalCount = buffer.getLong64();
            for (int j = 0; j < internalCount; j++) {
                BigInteger from = buffer.getUlong64();
                BigInteger to = buffer.getUlong64().subtract(BigInteger.valueOf(1));
                sb.append(":").append(from).append("-").append(to);
            }
        }
        event.setGtidSet(sb.toString());
        return event;
    }

    public static byte[] getBody(String gtidSet) {
        GtidSetPosition pos = new GtidSetPosition(gtidSet);
        // calculate body size
        int bodySize = 8;
        Collection<GtidSetPosition.UUIDSet> uuids = pos.getUUIDSets();
        for (GtidSetPosition.UUIDSet uuidSet : uuids) {
            bodySize += 16 + 8;
            bodySize += 16 * uuidSet.getIntervals().size();
        }
        // fill body
        byte[] body = new byte[bodySize];
        int offset = 0;
        ByteUtil.int8store(body, offset, uuids.size());
        offset += 8;
        for (GtidSetPosition.UUIDSet uuidSet : uuids) {
            byte[] uuidByte = parseUuid(uuidSet.getUUID());
            System.arraycopy(uuidByte, 0, body, offset, 16);
            offset += 16;
            ByteUtil.int8store(body, offset, uuidSet.getIntervals().size());
            offset += 8;
            for (GtidSetPosition.Interval interval : uuidSet.getIntervals()) {
                ByteUtil.int8store(body, offset, interval.getStart());
                offset += 8;
                ByteUtil.int8store(body, offset, interval.getEnd() + 1);
                offset += 8;
            }
        }
        return body;
    }

    private static byte[] parseUuid(String uuid) {
        byte[] source_id = new byte[16];
        int array_index = 0;
        int str_index = 0;
        while (str_index < uuid.length()) {
            char high_char = uuid.charAt(str_index++);
            if (high_char == '-') {
                continue;
            }
            byte high = char2Byte(high_char);
            char low_char = uuid.charAt(str_index++);
            byte low = char2Byte(low_char);
            source_id[array_index++] = (byte) (high * 16 + low);
        }
        return source_id;
    }

    private String parseServerId(byte[] sourcId) {
        StringBuilder sb = new StringBuilder();
        if (sourcId == null || sourcId.length <= 0) {
            return null;
        }
        for (int i = 0; i < sourcId.length; i++) {
            int v = sourcId[i] & 0xff;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                sb.append(0);
            }
            sb.append(hv);
            if (i == 3 | i == 5 | i == 7 | i == 9) {
                sb.append("-");
            }
        }
        return sb.toString();
    }

    private static byte char2Byte(char c) {
        byte b = 0;
        switch (c) {
            case '0':
                b = 0;
                break;
            case '1':
                b = 1;
                break;
            case '2':
                b = 2;
                break;
            case '3':
                b = 3;
                break;
            case '4':
                b = 4;
                break;
            case '5':
                b = 5;
                break;
            case '6':
                b = 6;
                break;
            case '7':
                b = 7;
                break;
            case '8':
                b = 8;
                break;
            case '9':
                b = 9;
                break;
            case 'a':
                b = 10;
                break;
            case 'b':
                b = 11;
                break;
            case 'c':
                b = 12;
                break;
            case 'd':
                b = 13;
                break;
            case 'e':
                b = 14;
                break;
            case 'f':
                b = 15;
                break;
            default:
                break;
        }
        return b;
    }

}
