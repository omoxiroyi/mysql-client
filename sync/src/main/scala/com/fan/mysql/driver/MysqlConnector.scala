package com.fan.mysql.driver

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicBoolean

import com.fan.mysql.config.Capabilities
import com.fan.mysql.packet.command.QuitPacket
import com.fan.mysql.packet.{AuthPacket, ErrorPacket, HandshakePacket, Scramble323Packet}
import com.fan.mysql.util._
import org.apache.commons.lang3.exception.ExceptionUtils

//noinspection DuplicatedCode
class MysqlConnector(address: InetSocketAddress,
                     username: String,
                     password: String) extends Logging {

  private[this] final val soTimeout = 30 * 1000

  private[this] final val receiveBufferSize = 8 * 1024 * 1024

  private[this] final val sendBufferSize = 8 * 1024 * 1024

  protected final val maxPacketSize = 32 * 1024 * 1024

  private[this] var channel: SocketChannel = _

  private[this] val connected: AtomicBoolean = new AtomicBoolean(false)

  private[this] var connectionId: Long = _

  // use utf8 charset as default
  private[this] val charsetNumber: Byte = 33

  private[this] var seed: String = _

  private[this] var defaultSchema: String = _

  var dumping = false

  /**
   * Connect to a MySQL server and do handshake.
   */
  def connect(): Unit = {
    if (connected.compareAndSet(false, true)) {
      try {
        channel = SocketChannel.open()
        configChannel(channel)
        logger.debug(s"connect MysqlConnection to $address")
        channel.socket().connect(address, 5000)
        negotiate(channel)
      } catch {
        case e: Exception =>
          throw new IOException(s"connect to ${address.toString} failure", e);
      }
    }
  }

  @throws[IOException]
  def negotiate(channel: SocketChannel): Unit = {
    val handshakeData = PacketManager.readNextPacket(channel)

    if (handshakeData(4) < 0) {
      if (handshakeData(4) == -1) {
        val error = new ErrorPacket
        error.init(new MySQLPacketBuffer(handshakeData), null)
        throw new IOException(s"handshake exception:\n${error.message}")
      } else if (handshakeData(4) == -2) {
        throw new IOException("Unexpected EOF packet at handshake phase.")
      } else {
        throw new IOException(s"Unexpected packet with field_count=${handshakeData(4)}")
      }
    }

    val packet = new HandshakePacket
    packet.init(new MySQLPacketBuffer(handshakeData), null)

    this.connectionId = packet.connectionId
    logger.debug("handshake initialization packet received, prepare the client authentication packet to send")

    val authPacket = new AuthPacket
    authPacket.packetId = (handshakeData(3) + 1).toByte
    authPacket.user = username
    authPacket.password = password
    authPacket.charsetIndex = this.charsetNumber
    authPacket.maxPacketSize = maxPacketSize
    authPacket.seed = packet.authPluginData1 + packet.authPluginData2
    this.seed = packet.authPluginData1

    authPacket.clientFlags = initClientFlags(packet)

    if (this.defaultSchema != null) {
      authPacket.clientFlags |= Capabilities.CLIENT_CONNECT_WITH_DB
      authPacket.database = this.defaultSchema
    }

    val authData = authPacket.toByteBuffer(getCharset).array

    PacketManager.write(channel, authData)
    logger.debug("client authentication packet is sent out.")

    // check auth result
    val authResponseData: Array[Byte] = PacketManager.readNextPacket(channel)
    if (authResponseData(4) < 0) {
      if (authResponseData(4) == -1) {
        val error: ErrorPacket = new ErrorPacket
        error.init(new MySQLPacketBuffer(authResponseData), getCharset)
        throw new IOException("Error when doing client authentication:" + error.message)
      }
      else {
        if (authResponseData(4) == -2) {
          handleScramble323Packet(authResponseData(3))
        }
        else {
          throw new IOException(s"Unexpected packet with field_count=${authResponseData(4)}")
        }
      }
    }
  }

  @throws[IOException]
  def disconnect(): Unit = {
    if (connected.compareAndSet(true, false)) {
      try {
        if (channel != null)
          channel.close()
        logger.debug(s"disConnect MysqlConnection to $address...")
      } catch {
        case e: Exception =>
          throw new IOException(s"disconnect ${this.address} failure:${ExceptionUtils.getStackTrace(e)}")
      }
    }
    else logger.info(s"the channel ${this.address} is not connected")
  }

  @throws[IOException]
  def reconnect(): Unit = {
    disconnect()
    connect()
  }

  def fork = new MysqlConnector(address, username, password)

  @throws[IOException]
  def quit(): Unit = {
    PacketManager.write(channel, QuitPacket.toByteBuffer(getCharset).array)
    if (this.channel != null) {
      this.channel.close()
    }
  }

  @throws[IOException]
  private def configChannel(channel: SocketChannel): Unit = {
    channel.socket().setKeepAlive(true)
    channel.socket().setReuseAddress(true)
    channel.socket().setSoTimeout(soTimeout)
    channel.socket().setTcpNoDelay(true)
    channel.socket().setReceiveBufferSize(receiveBufferSize)
    channel.socket().setSendBufferSize(sendBufferSize)
  }

  private def initClientFlags(packet: HandshakePacket) = {
    var flag = 0
    flag |= Capabilities.CLIENT_FOUND_ROWS
    flag |= Capabilities.CLIENT_PROTOCOL_41
    // Need this to get server status values
    flag |= Capabilities.CLIENT_TRANSACTIONS
    // We always allow multiple result sets
    flag |= Capabilities.CLIENT_MULTI_RESULTS
    // delimiter
    flag |= Capabilities.CLIENT_MULTI_STATEMENTS
    flag |= Capabilities.CLIENT_IGNORE_SPACE
    flag |= Capabilities.CLIENT_INTERACTIVE
    flag |= Capabilities.CLIENT_IGNORE_SIGPIPE
    // load local file
    flag |= Capabilities.CLIENT_LOCAL_FILES
    if (packet.protocolVersion > 9) flag |= Capabilities.CLIENT_LONG_PASSWORD
    if ((packet.lowerCapabilities & Capabilities.CLIENT_LONG_FLAG) != 0) flag |= Capabilities.CLIENT_LONG_FLAG
    if ((packet.lowerCapabilities & Capabilities.CLIENT_SECURE_CONNECTION) != 0) flag |= Capabilities.CLIENT_SECURE_CONNECTION
    flag
  }

  def getCharset: String = CharsetUtil.getCharsetFromIndex(charsetNumber)

  @throws[IOException]
  private def handleScramble323Packet(packetId: Byte): Unit = {
    val packet: Scramble323Packet = new Scramble323Packet
    packet.packetId = (packetId + 1).toByte
    packet.seed = SecurityUtil.scramble323(password, this.seed)
    val b323data = packet.toByteBuffer(this.getCharset).array
    PacketManager.write(channel, b323data)
    logger.debug("client 323 authentication packet is sent out.")
    // check auth result
    val authResponseData = PacketManager.readNextPacket(channel)
    authResponseData(4) match {
      case 0 =>

      case -1 =>
        val error: ErrorPacket = new ErrorPacket
        error.init(new MySQLPacketBuffer(authResponseData), getCharset)
        throw new IOException("Error When doing Client Authentication:" + error.message)
      case _ =>
        throw new IOException("Unexpected packet with field_count=" + authResponseData(4))
    }
  }

  def isConnected: Boolean = this.channel != null && this.channel.isConnected

  def getChannel: SocketChannel = channel
}
