package com.fan.mysql.async.exceptions

import io.netty.channel.ChannelFuture

class CanceledChannelFutureException(val channelFuture: ChannelFuture)
    extends IllegalStateException("This channel future was canceled -> %s".format(channelFuture))
