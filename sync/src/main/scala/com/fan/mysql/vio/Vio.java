package com.fan.mysql.vio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Vio {

    private final SocketChannel channel;

    private final ByteBuffer read_buffer;
    private int read_pos;
    private int read_end;
    private volatile long last_read_time;

    public Vio(SocketChannel channel) {
        this.channel = channel;
        int VIO_READ_BUFFER_SIZE = 16384;
        this.read_buffer = ByteBuffer.allocate(VIO_READ_BUFFER_SIZE);
        this.last_read_time = -1;
    }

    public int vio_read_buff(byte[] dest, int offset, int size) throws IOException {
        int receive_size = 0;
        int VIO_UNBUFFERED_READ_MIN_SIZE = 2048;
        if (read_pos < read_end) {
            // read cache data
            receive_size = Math.min(read_end - read_pos, size);
            System.arraycopy(read_buffer.array(), read_pos, dest, offset, receive_size);
            read_pos += receive_size;
        } else if (size < VIO_UNBUFFERED_READ_MIN_SIZE) {
            // read into buffer
            read_buffer.clear();
            int read_size = channel_read(read_buffer);
            receive_size = Math.min(read_size, size);
            System.arraycopy(read_buffer.array(), 0, dest, offset, receive_size);
            read_pos = receive_size;
            read_end = read_size;
        } else {
            // read directly
            ByteBuffer buf = ByteBuffer.wrap(dest, offset, size);
            receive_size = channel_read(buf);
        }
        return receive_size;
    }

    public long getLastReadTime() {
        return last_read_time;
    }

    private int channel_read(ByteBuffer buf) throws IOException {
        last_read_time = System.currentTimeMillis();
        int receive_size = channel.read(buf);
        last_read_time = -1;
        if (receive_size == -1) {
            throw new IOException("Unexpected End Stream");
        }
        return receive_size;
    }
}
