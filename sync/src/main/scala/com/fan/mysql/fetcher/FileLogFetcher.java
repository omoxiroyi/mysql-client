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

public class FileLogFetcher extends LogBuffer {

    private static final Logger logger = LoggerFactory.getLogger(FileLogFetcher.class);

    private static int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);
    private static int EVENT_LENGTH_OFFSET = 9;
    private static int EVENT_HEADER_LEN = 19;

    private final String binlogFilePath;
    private final BlockingQueue<LogBuffer> queue;

    private Thread self;
    private int readOffset;

    public FileLogFetcher(String binlogFilePath, BlockingQueue<LogBuffer> queue) {
        this.binlogFilePath = binlogFilePath;
        this.queue = queue;
        this.buffer = new byte[MAX_PACKET_LENGTH];
    }

    public void start() throws IOException {
        this.self = new Thread(new Runnable() {

            @Override
            public void run() {
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

            }
        }, "Binlog fetcher thread");
        self.start();
    }

    public LogBuffer fetch(FileChannel channel) throws IOException {
        // fetch header
        boolean success = fetch0(channel, EVENT_HEADER_LEN);
        if (!success) {
            return null;
        }
        position = limit - EVENT_HEADER_LEN + EVENT_LENGTH_OFFSET;
        int eventLen = (int) getUint32();
        // fetch event
        success = fetch0(channel, eventLen - EVENT_HEADER_LEN);
        if (!success) {
            logger.error("Unexcepted end of file");
            return null;
        }
        LogBuffer buf = new LogBuffer(Arrays.copyOfRange(buffer, limit - eventLen, limit), 0, eventLen);
        return buf;
    }

    private final boolean fetch0(FileChannel channel, final int len) throws IOException {
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
