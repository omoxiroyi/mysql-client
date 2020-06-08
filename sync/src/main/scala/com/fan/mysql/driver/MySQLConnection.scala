package com.fan.mysql.driver

import java.io.IOException
import java.net.InetSocketAddress

import com.fan.mysql.packet.OkPacket
import com.fan.mysql.packet.result.ResultSetPacket

class MySQLConnection(address: InetSocketAddress,
                      username: String,
                      password: String) {

  def this(host: String, port: Int, username: String, password: String) =
    this(new InetSocketAddress(host, port), username, password)

  private[this] val connector: MysqlConnector = new MysqlConnector(address, username, password)

  @throws[IOException]
  def connect(): Unit = {
    connector.connect()
  }

  @throws[IOException]
  def reconnect(): Unit = {
    connector.reconnect()
  }

  @throws[IOException]
  def disconnect(): Unit = {
    connector.disconnect()
  }

  def isConnected: Boolean = connector.isConnected

  @throws[IOException]
  def query(cmd: String): ResultSetPacket = {
    val executor = new MysqlQueryExecutor(connector)
    executor.query(cmd)
  }

  @throws[IOException]
  def query(cmd: String, queryHandler: MysqlConnQueryHandler): Unit = {
    val executor = new MysqlQueryExecutor(connector)
    executor.query(cmd, queryHandler)
  }

  @throws[IOException]
  def update(cmd: String): OkPacket = {
    val executor = new MysqlUpdateExecutor(connector)
    executor.update(cmd)
  }

  def quit(): Unit = connector.quit()

}
