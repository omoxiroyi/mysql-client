package com.fan.mysql.async.codec

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import com.fan.mysql.async.binary.BinaryRowDecoder
import com.fan.mysql.async.db.Configuration
import com.fan.mysql.async.exceptions.DatabaseException
import com.fan.mysql.async.general.MutableResultSet
import com.fan.mysql.async.message.client._
import com.fan.mysql.async.message.server._
import com.fan.mysql.async.util.ChannelFutureTransformer._
import com.fan.mysql.async.util.{CharsetMapper, Log}
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}
import io.netty.channel._
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.CodecException
import org.slf4j.Logger

import scala.annotation.switch
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

class MySQLConnectionHandler(
                              configuration: Configuration,
                              charsetMapper: CharsetMapper,
                              handlerDelegate: MySQLHandlerDelegate,
                              group: EventLoopGroup,
                              executionContext: ExecutionContext,
                              connectionId: String
                            ) extends SimpleChannelInboundHandler[Any] {

  private[this] implicit val internalPool: ExecutionContext = executionContext
  private[this] final val log: Logger = Log.getByName(s"[connection-handler]$connectionId")
  private[this] final val bootstrap: Bootstrap = new Bootstrap().group(this.group)
  private[this] final val decoder: MySQLPacketDecoder = new MySQLPacketDecoder(configuration.charset, connectionId)
  private[this] final val encoder: MySQLOneToOneEncoder = new MySQLOneToOneEncoder(configuration.charset, charsetMapper)
  private[this] final val sendLongDataEncoder = new SendLongDataEncoder()
  private[this] final val connectionPromise: Promise[MySQLConnectionHandler] = Promise[MySQLConnectionHandler]

  private[this] final val currentParameters = new ArrayBuffer[ColumnDefinitionMessage]()
  private[this] final val currentColumns = new ArrayBuffer[ColumnDefinitionMessage]()
  private[this] final val parsedStatements = new mutable.HashMap[String, PreparedStatementHolder]()
  private[this] final val binaryRowDecoder = new BinaryRowDecoder()

  private[this] var currentPreparedStatementHolder: PreparedStatementHolder = _
  private[this] var currentPreparedStatement: PreparedStatement = _
  private[this] var currentQuery: MutableResultSet[ColumnDefinitionMessage] = _
  private[this] var currentContext: ChannelHandlerContext = _


  def connect(): Future[MySQLConnectionHandler] = {
    this.bootstrap.channel(classOf[NioSocketChannel])

    this.bootstrap.handler(new ChannelInitializer[Channel] {
      override def initChannel(channel: Channel): Unit = {
        channel.pipeline.addLast(
          decoder,
          encoder,
          sendLongDataEncoder, // used in prepared statement.
          MySQLConnectionHandler.this
        )
      }
    })

    this.bootstrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    this.bootstrap.option[ByteBufAllocator](ChannelOption.ALLOCATOR, LittleEndianByteBufAllocator.INSTANCE)

    this.bootstrap.connect(new InetSocketAddress(configuration.host, configuration.port)).onComplete {
      case Failure(exception) => this.connectionPromise.tryFailure(exception)
      case _ =>
    }

    this.connectionPromise.future
  }

  override def channelRead0(ctx: ChannelHandlerContext, message: Any) {
    message match {
      case m: ServerMessage =>
        (m.kind: @switch) match {
          case ServerMessage.ServerProtocolVersion =>
            handlerDelegate.onHandshake(m.asInstanceOf[HandshakeMessage])
          case ServerMessage.Ok =>
            this.clearQueryState()
            handlerDelegate.onOk(m.asInstanceOf[OkMessage])
          case ServerMessage.Error =>
            this.clearQueryState()
            handlerDelegate.onError(m.asInstanceOf[ErrorMessage])
          case ServerMessage.EOF =>
            this.handleEOF(m)
          case ServerMessage.ColumnDefinition =>
            val message = m.asInstanceOf[ColumnDefinitionMessage]

            if (currentPreparedStatementHolder != null && this.currentPreparedStatementHolder.needsAny) {
              this.currentPreparedStatementHolder.add(message)
            }

            this.currentColumns += message
          case ServerMessage.ColumnDefinitionFinished =>
            this.onColumnDefinitionFinished()
          case ServerMessage.PreparedStatementPrepareResponse =>
            this.onPreparedStatementPrepareResponse(m.asInstanceOf[PreparedStatementPrepareResponse])
          case ServerMessage.Row =>
            val message = m.asInstanceOf[ResultSetRowMessage]
            val items = new Array[Any](message.size)

            var x = 0
            while (x < message.size) {
              items(x) = if (message(x) == null) {
                null
              } else {
                val columnDescription = this.currentQuery.columnTypes(x)
                columnDescription.textDecoder.decode(columnDescription, message(x), configuration.charset)
              }
              x += 1
            }

            this.currentQuery.addRow(items)
          case ServerMessage.BinaryRow =>
            val message = m.asInstanceOf[BinaryRowMessage]
            this.currentQuery.addRow(this.binaryRowDecoder.decode(message.buffer, this.currentColumns.toSeq))
          case ServerMessage.ParamProcessingFinished =>
          case ServerMessage.ParamAndColumnProcessingFinished =>
            this.onColumnDefinitionFinished()
        }
    }

  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    log.debug("Channel became active")
    handlerDelegate.connected(ctx)
  }


  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    log.debug("Channel became inactive")
  }

  //noinspection ScalaDeprecation
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    // unwrap CodecException if needed
    cause match {
      case t: CodecException => handleException(t.getCause)
      case _ => handleException(cause)
    }

  }

  private def handleException(cause: Throwable) {
    if (!this.connectionPromise.isCompleted) {
      this.connectionPromise.failure(cause)
    }
    handlerDelegate.exceptionCaught(cause)
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    this.currentContext = ctx
  }

  def write(message: QueryMessage): ChannelFuture = {
    this.decoder.queryProcessStarted()
    writeAndHandleError(message)
  }

  def sendPreparedStatement(query: String, values: Seq[Any]): Future[ChannelFuture] = {
    val preparedStatement = PreparedStatement(query, values)

    this.currentColumns.clear()
    this.currentParameters.clear()

    this.currentPreparedStatement = preparedStatement

    this.parsedStatements.get(preparedStatement.statement) match {
      case Some(item) =>
        this.executePreparedStatement(item.statementId, item.columns.size, preparedStatement.values, item.parameters.toSeq)
      case None =>
        decoder.preparedStatementPrepareStarted()
        writeAndHandleError(PreparedStatementPrepareMessage(preparedStatement.statement))
    }
  }

  def write(message: BinlogDumpMessage): ChannelFuture = {
    decoder.isInDumping = true
    writeAndHandleError(message)
  }

  def write(message: BinlogDumpGTIDMessage): ChannelFuture = {
    writeAndHandleError(message)
  }

  def write(message: HandshakeResponseMessage): ChannelFuture = {
    decoder.hasDoneHandshake = true
    writeAndHandleError(message)
  }

  def write(message: AuthenticationSwitchResponse): ChannelFuture = writeAndHandleError(message)

  def write(message: QuitMessage): ChannelFuture = {
    writeAndHandleError(message)
  }

  def disconnect: ChannelFuture = this.currentContext.close()

  def clearQueryState(): Unit = {
    this.currentColumns.clear()
    this.currentParameters.clear()
    this.currentQuery = null
  }

  def isConnected: Boolean = {
    if (this.currentContext != null && this.currentContext.channel() != null) {
      this.currentContext.channel.isActive
    } else {
      false
    }
  }

  private def executePreparedStatement(statementId: Array[Byte], columnsCount: Int, values: scala.Seq[Any], parameters: Seq[ColumnDefinitionMessage]): Future[ChannelFuture] = {
    decoder.preparedStatementExecuteStarted(columnsCount, parameters.size)
    this.currentColumns.clear()
    this.currentParameters.clear()

    val (nonLongIndicesOpt, longValuesOpt) = values.zipWithIndex.map {
      case (Some(value), index) if isLong(value) => (None, Some(index, value))
      case (value, index) if isLong(value) => (None, Some(index, value))
      case (_, index) => (Some(index), None)
    }.unzip
    val nonLongIndices: Seq[Int] = nonLongIndicesOpt.flatten
    val longValues: Seq[(Int, Any)] = longValuesOpt.flatten

    if (longValues.nonEmpty) {
      val (firstIndex, firstValue) = longValues.head
      var channelFuture: Future[ChannelFuture] = sendLongParameter(statementId, firstIndex, firstValue)
      longValues.tail foreach { case (index, value) =>
        channelFuture = channelFuture.flatMap { _ =>
          sendLongParameter(statementId, index, value)
        }
      }
      channelFuture flatMap { _ =>
        writeAndHandleError(PreparedStatementExecuteMessage(statementId, values, nonLongIndices.toSet, parameters))
      }
    } else {
      writeAndHandleError(PreparedStatementExecuteMessage(statementId, values, nonLongIndices.toSet, parameters))
    }
  }

  private def isLong(value: Any): Boolean = {
    value match {
      case v: Array[Byte] => v.length > SendLongDataEncoder.LONG_THRESHOLD
      case v: ByteBuffer => v.remaining() > SendLongDataEncoder.LONG_THRESHOLD
      case v: ByteBuf => v.readableBytes() > SendLongDataEncoder.LONG_THRESHOLD

      case _ => false
    }
  }

  private def sendLongParameter(statementId: Array[Byte], index: Int, longValue: Any): Future[ChannelFuture] = {
    longValue match {
      case v: Array[Byte] =>
        sendBuffer(Unpooled.wrappedBuffer(v), statementId, index)

      case v: ByteBuffer =>
        sendBuffer(Unpooled.wrappedBuffer(v), statementId, index)

      case v: ByteBuf =>
        sendBuffer(v, statementId, index)
    }
  }

  private def sendBuffer(buffer: ByteBuf, statementId: Array[Byte], paramId: Int): ChannelFuture = {
    writeAndHandleError(SendLongDataMessage(statementId, buffer, paramId))
  }

  private def onPreparedStatementPrepareResponse(message: PreparedStatementPrepareResponse) {
    this.currentPreparedStatementHolder = new PreparedStatementHolder(this.currentPreparedStatement.statement, message)
  }

  def onColumnDefinitionFinished(): Unit = {

    val columns = if (this.currentPreparedStatementHolder != null) {
      this.currentPreparedStatementHolder.columns
    } else {
      this.currentColumns
    }

    this.currentQuery = new MutableResultSet[ColumnDefinitionMessage](columns.toIndexedSeq)

    if (this.currentPreparedStatementHolder != null) {
      this.parsedStatements.put(this.currentPreparedStatementHolder.statement, this.currentPreparedStatementHolder)
      this.executePreparedStatement(
        this.currentPreparedStatementHolder.statementId,
        this.currentPreparedStatementHolder.columns.size,
        this.currentPreparedStatement.values,
        this.currentPreparedStatementHolder.parameters.toSeq
      )
      this.currentPreparedStatementHolder = null
      this.currentPreparedStatement = null
    }
  }

  private def writeAndHandleError(message: Any): ChannelFuture = {
    if (this.currentContext.channel().isActive) {
      val res = this.currentContext.writeAndFlush(message)

      res.onComplete {
        case Failure(e) => handleException(e)
        case _ =>
      }

      res
    } else {
      val error = new DatabaseException("This channel is not active and can't take messages")
      handleException(error)
      this.currentContext.channel().newFailedFuture(error)
    }
  }

  private def handleEOF(m: ServerMessage): Unit = {
    m match {
      case eof: EOFMessage =>
        val resultSet = this.currentQuery
        this.clearQueryState()

        if (resultSet != null) {
          handlerDelegate.onResultSet(resultSet, eof)
        } else {
          handlerDelegate.onEOF(eof)
        }
      case authenticationSwitch: AuthenticationSwitchRequest =>
        handlerDelegate.switchAuthentication(authenticationSwitch)
    }
  }

}
