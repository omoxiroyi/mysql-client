package com.fan.mysql.async.db

import java.nio.charset.Charset

import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator}
import io.netty.util.CharsetUtil

import scala.concurrent.duration._

object Configuration {
  val DefaultCharset: Charset = CharsetUtil.UTF_8

  @deprecated("Use URLParser.DEFAULT.", since = "0.2.20")
  val Default = new Configuration(
    "root",
    "127.0.0.1",
    3306,
    null,
    null)
}

/**
 *
 * Contains the configuration necessary to connect to a database.
 *
 * @param username           database username
 * @param host               database host, defaults to "localhost"
 * @param port               database port, defaults to 5432
 * @param password           password, defaults to no password
 * @param database           database name, defaults to no database
 * @param ssl                ssl configuration
 * @param charset            charset for the connection, defaults to UTF-8, make sure you know what you are doing if you
 *                           change this
 * @param maximumMessageSize the maximum size a message from the server could possibly have, this limits possible
 *                           OOM or eternal loop attacks the client could have, defaults to 16 MB. You can set this
 *                           to any value you would like but again, make sure you know what you are doing if you do
 *                           change it.
 * @param allocator          the netty buffer allocator to be used
 * @param connectTimeout     the timeout for connecting to servers
 * @param testTimeout        the timeout for connection tests performed by pools
 * @param queryTimeout       the optional query timeout
 *
 */
case class Configuration(
                          username: String,
                          host: String = "localhost",
                          port: Int = 3306,
                          password: Option[String] = None,
                          database: Option[String] = None,
                          ssl: SSLConfiguration = SSLConfiguration(),
                          charset: Charset = Configuration.DefaultCharset,
                          maximumMessageSize: Int = 16777216,
                          allocator: ByteBufAllocator = PooledByteBufAllocator.DEFAULT,
                          connectTimeout: Duration = 5.seconds,
                          testTimeout: Duration = 5.seconds,
                          queryTimeout: Option[Duration] = None
                        ) {

  // convince for Java constructor
  def this(username: String,
           host: String,
           port: Int,
           password: String,
           database: String) = {
    this(username, host, port, Option(password), Option(database))
  }
}
