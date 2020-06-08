package com.fan.mysql.async.codec

import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

import com.fan.mysql.async.decoder._
import com.fan.mysql.async.exceptions.{BufferNotFullyConsumedException, NegativeMessageSizeException, ParserNotAvailableException}
import com.fan.mysql.async.message.server._
import com.fan.mysql.async.util.ByteBufferUtils._
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.{BufferDumper, Log}
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import org.slf4j.Logger

class MySQLPacketDecoder(charset: Charset, connectionId: String) extends ByteToMessageDecoder {

  private[this] final val log: Logger = Log.getByName(s"[packet-decoder]$connectionId")
  private[this] final val messagesCount: AtomicInteger = new AtomicInteger()

  // decoder
  private[this] final val errorDecoder: ErrorDecoder = new ErrorDecoder(charset)
  private[this] final val okDecoder = new OkDecoder(charset)
  private[this] final val handshakeDecoder = new HandshakeV10Decoder(charset)

  private[this] final val columnDecoder = new ColumnDefinitionDecoder(charset, new DecoderRegistry(charset))
  private[this] final val rowDecoder = new ResultSetRowDecoder(charset)
  private[this] final val preparedStatementPrepareDecoder = new PreparedStatementPrepareResponseDecoder()

  private[codec] var processingColumns = false
  private[codec] var processingParams = false
  private[codec] var isInQuery = false
  private[codec] var isPreparedStatementPrepare = false
  private[codec] var isPreparedStatementExecute = false
  private[codec] var isPreparedStatementExecuteRows = false
  private[codec] var hasDoneHandshake = false

  private[codec] var totalParams = 0L
  private[codec] var processedParams = 0L
  private[codec] var totalColumns = 0L
  private[codec] var processedColumns = 0L

  private[this] var hasReadColumnsCount = false

  def decode(ctx: ChannelHandlerContext, buffer: ByteBuf, out: java.util.List[Object]): Unit = {
    if (buffer.readableBytes() > 4) {

      buffer.markReaderIndex()

      val size = read3BytesInt(buffer)

      buffer.readUnsignedByte() // we have to read this

      if (buffer.readableBytes() >= size) {

        messagesCount.incrementAndGet()

        val messageType = buffer.getByte(buffer.readerIndex())

        if (size < 0) {
          throw new NegativeMessageSizeException(messageType, size)
        }

        val slice = buffer.readSlice(size)

        if (log.isTraceEnabled) {
          log.trace(s"Reading message type $messageType - " +
            s"(count=$messagesCount,hasDoneHandshake=$hasDoneHandshake,size=$size,isInQuery=$isInQuery,processingColumns=$processingColumns,processingParams=$processingParams,processedColumns=$processedColumns,processedParams=$processedParams)" +
            s"\n${BufferDumper.dumpAsHex(slice)}}")
        }

        slice.readByte()

        if (this.hasDoneHandshake) {
          this.handleCommonFlow(messageType, slice, out)
        } else {
          val decoder = messageType match {
            case ServerMessage.Error =>
              this.clear()
              this.errorDecoder
            case _ => this.handshakeDecoder
          }
          this.doDecoding(decoder, slice, out)
        }
      } else {
        buffer.resetReaderIndex()
      }

    }
  }

  private def handleCommonFlow(messageType: Byte, slice: ByteBuf, out: java.util.List[Object]) {
    val decoder = messageType match {
      case ServerMessage.Error =>
        this.clear()
        this.errorDecoder
      case ServerMessage.EOF =>

        if (this.processingParams && this.totalParams > 0) {
          this.processingParams = false
          if (this.totalColumns == 0) {
            ParamAndColumnProcessingFinishedDecoder
          } else {
            ParamProcessingFinishedDecoder
          }
        } else {
          if (this.processingColumns) {
            this.processingColumns = false
            ColumnProcessingFinishedDecoder
          } else {
            this.clear()
            EOFMessageDecoder
          }
        }
      case ServerMessage.Ok =>
        if (this.isPreparedStatementPrepare) {
          this.preparedStatementPrepareDecoder
        } else {
          if (this.isPreparedStatementExecuteRows) {
            null
          } else {
            this.clear()
            this.okDecoder
          }
        }
      case _ =>

        if (this.isInQuery) {
          null
        } else {
          throw new ParserNotAvailableException(messageType)
        }
    }

    doDecoding(decoder, slice, out)
  }

  private def doDecoding(decoder: MessageDecoder, slice: ByteBuf, out: java.util.List[Object]) {
    if (decoder == null) {
      slice.readerIndex(slice.readerIndex() - 1)
      val result = decodeQueryResult(slice)

      if (slice.readableBytes() != 0) {
        throw new BufferNotFullyConsumedException(slice)
      }
      if (result != null) {
        out.add(result)
      }
    } else {
      val result = decoder.decode(slice)

      result match {
        case m: PreparedStatementPrepareResponse =>
          this.hasReadColumnsCount = true
          this.totalColumns = m.columnsCount
          this.totalParams = m.paramsCount
        case _: ParamAndColumnProcessingFinishedMessage =>
          this.clear()
        case _: ColumnProcessingFinishedMessage if this.isPreparedStatementPrepare =>
          this.clear()
        case _: ColumnProcessingFinishedMessage if this.isPreparedStatementExecute =>
          this.isPreparedStatementExecuteRows = true
        case _ =>
      }

      if (slice.readableBytes() != 0) {
        throw new BufferNotFullyConsumedException(slice)
      }

      if (result != null) {
        result match {
          case m: PreparedStatementPrepareResponse =>
            out.add(result)
            if (m.columnsCount == 0 && m.paramsCount == 0) {
              this.clear()
              out.add(ParamAndColumnProcessingFinishedMessage(EOFMessage(0, 0)))
            }
          case _ => out.add(result)
        }
      }
    }
  }

  private def decodeQueryResult(slice: ByteBuf): AnyRef = {
    if (!hasReadColumnsCount) {
      this.hasReadColumnsCount = true
      this.totalColumns = slice.readBinaryLength
      return null
    }

    if (this.processingParams && this.totalParams != this.processedParams) {
      this.processedParams += 1
      return this.columnDecoder.decode(slice)
    }


    if (this.totalColumns == this.processedColumns) {
      if (this.isPreparedStatementExecute) {
        val row = slice.readBytes(slice.readableBytes())
        row.readByte() // reads initial 00 at message
        BinaryRowMessage(row)
      } else {
        this.rowDecoder.decode(slice)
      }
    } else {
      this.processedColumns += 1
      this.columnDecoder.decode(slice)
    }

  }

  def preparedStatementPrepareStarted() {
    this.queryProcessStarted()
    this.hasReadColumnsCount = true
    this.processingParams = true
    this.processingColumns = true
    this.isPreparedStatementPrepare = true
  }

  def preparedStatementExecuteStarted(columnsCount: Int, paramsCount: Int) {
    this.queryProcessStarted()
    this.hasReadColumnsCount = false
    this.totalColumns = columnsCount
    this.totalParams = paramsCount
    this.isPreparedStatementExecute = true
    this.processingParams = false
  }

  def queryProcessStarted() {
    this.isInQuery = true
    this.processingColumns = true
    this.hasReadColumnsCount = false
  }

  private def clear() {
    this.isPreparedStatementPrepare = false
    this.isPreparedStatementExecute = false
    this.isPreparedStatementExecuteRows = false
    this.isInQuery = false
    this.processingColumns = false
    this.processingParams = false
    this.totalColumns = 0
    this.processedColumns = 0
    this.totalParams = 0
    this.processedParams = 0
    this.hasReadColumnsCount = false
  }
}
