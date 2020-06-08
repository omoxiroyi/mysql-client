package com.fan.mysql.async.codec

import java.nio.charset.Charset

import com.fan.mysql.async.binary.BinaryRowEncoder
import com.fan.mysql.async.encoder._
import com.fan.mysql.async.exceptions.EncoderNotAvailableException
import com.fan.mysql.async.message.client.ClientMessage
import com.fan.mysql.async.util.{BufferDumper, ByteBufferUtils, CharsetMapper, Log}
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import org.slf4j.Logger

import scala.annotation.switch

object MySQLOneToOneEncoder {
  val log: Logger = Log.get[MySQLOneToOneEncoder]
}

class MySQLOneToOneEncoder(charset: Charset, charsetMapper: CharsetMapper)
  extends MessageToMessageEncoder[ClientMessage](classOf[ClientMessage]) {

  import MySQLOneToOneEncoder.log

  private[this] final val handshakeResponseEncoder = new HandshakeResponseEncoder(charset, charsetMapper)
  private final val queryEncoder = new QueryMessageEncoder(charset)
  private final val rowEncoder = new BinaryRowEncoder(charset)
  private final val prepareEncoder = new PreparedStatementPrepareEncoder(charset)
  private final val executeEncoder = new PreparedStatementExecuteEncoder(rowEncoder)
  private final val authenticationSwitchEncoder = new AuthenticationSwitchResponseEncoder(charset)

  private[this] var sequence = 1

  def encode(ctx: ChannelHandlerContext, message: ClientMessage, out: java.util.List[Object]): Unit = {
    val encoder = (message.kind: @switch) match {
      case ClientMessage.ClientProtocolVersion => this.handshakeResponseEncoder
      case ClientMessage.Quit =>
        sequence = 0
        QuitMessageEncoder
      case ClientMessage.Query =>
        sequence = 0
        this.queryEncoder
      case ClientMessage.PreparedStatementExecute =>
        sequence = 0
        this.executeEncoder
      case ClientMessage.PreparedStatementPrepare =>
        sequence = 0
        this.prepareEncoder
      case ClientMessage.AuthSwitchResponse =>
        sequence += 1
        this.authenticationSwitchEncoder
      case _ => throw new EncoderNotAvailableException(message)
    }

    val result: ByteBuf = encoder.encode(message)

    ByteBufferUtils.writePacketLength(result, sequence)

    sequence += 1

    if (log.isTraceEnabled) {
      log.trace(s"Writing message ${message.getClass.getName} - \n${BufferDumper.dumpAsHex(result)}")
    }

    out.add(result)
  }
}