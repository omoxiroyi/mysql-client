import java.nio.ByteOrder

import io.netty.buffer.Unpooled

val buffer = Unpooled.buffer(1024).order(ByteOrder.LITTLE_ENDIAN)

buffer.writerIndex(0)

buffer.writeByte(0x01)
buffer.writeByte(0x02)
buffer.writeByte(0x01)
buffer.writeByte(0x01)

println(buffer.readInt())

