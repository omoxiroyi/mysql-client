package com.fan.mysql.async.util

import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}

object NettyUtils {
  InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)

  lazy val DefaultEventLoopGroup = new NioEventLoopGroup(0, DaemonThreadsFactory("db-async-netty"))
}
