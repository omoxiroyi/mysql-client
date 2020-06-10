package com.fan.mysql.fetcher;

import com.fan.mysql.dbsync.LogBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

@SuppressWarnings("unused")
public class FileLogFetcher extends LogBuffer {

    private static final Logger logger = LoggerFactory.getLogger(FileLogFetcher.class);

    private final String binlogFilePath;
    private final BlockingQueue<LogBuffer> queue;

    private int readOffset;

    public FileLogFetcher(String binlogFilePath, BlockingQueue<LogBuffer> queue) {
        this.binlogFilePath = binlogFilePath;
        this.queue = queue;
        int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);
        this.buffer = new byte[MAX_PACKET_LENGTH];
    }

    public void start() {
        // read magic number
        // read binlog events
        Thread self = new Thread(() -> {
            FileInputStream fis;
            FileChannel channel;
            try {
                fis = new FileInputStream(binlogFilePath);
                channel = fis.getChannel();
                // read magic number
                fetch0(channel, 4);
                // read binlog events
                while (true) {
                    LogBuffer buf = fetch(channel);
                    if (buf == null) {
                        break;
                    }
                    try {
                        queue.put(buf);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                queue.put(LogBuffer.EOF);
            } catch (Exception e) {
                logger.error("fetch binlog from file error: {}", binlogFilePath, e);
            }

        }, "Binlog fetcher thread");
        self.start();
    }

    public LogBuffer fetch(FileChannel channel) throws IOException {
        // fetch header
        int EVENT_HEADER_LEN = 19;
        boolean success = fetch0(channel, EVENT_HEADER_LEN);
        if (!success) {
            return null;
        }
        int EVENT_LENGTH_OFFSET = 9;
        position = limit - EVENT_HEADER_LEN + EVENT_LENGTH_OFFSET;
        int eventLen = (int) getUint32();
        // fetch event
        success = fetch0(channel, eventLen - EVENT_HEADER_LEN);
        if (!success) {
            logger.error("Unexcepted end of file");
            return null;
        }
        return new LogBuffer(Arrays.copyOfRange(buffer, limit - eventLen, limit), 0, eventLen);
    }

    private boolean fetch0(FileChannel channel, final int len) throws IOException {
        ensureCapacity(len);
        ByteBuffer buf = ByteBuffer.wrap(buffer, readOffset, buffer.length - readOffset);
        int end = limit + len;
        while (readOffset < end) {
            int readNum = channel.read(buf);
            if (readNum == -1) {
                logger.info("End of file");
                return false;
            }
            readOffset += readNum;
        }
        limit = end;
        return true;
    }

    private void ensureCapacity(final int len) {
        if (limit + len > buffer.length) {
            // reset buffer
            System.arraycopy(buffer, 0, buffer, 0, readOffset);
        }
    }

}
