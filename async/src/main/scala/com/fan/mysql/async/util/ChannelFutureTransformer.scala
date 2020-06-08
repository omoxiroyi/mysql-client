

package com.fan.mysql.async.util

import com.fan.mysql.async.exceptions.CanceledChannelFutureException
import io.netty.channel.ChannelFuture

import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions

object ChannelFutureTransformer {

  implicit def toScalaFuture(channelFuture: ChannelFuture): Future[ChannelFuture] = {
    val promise = Promise[ChannelFuture]

    channelFuture.addListener((future: ChannelFuture) => {
      if (future.isSuccess) {
        promise.success(future)
      } else {
        val exception = if (future.cause == null) {
          new CanceledChannelFutureException(future).fillInStackTrace()
        } else {
          future.cause
        }

        promise.failure(exception)
      }
    })

    promise.future
  }

}
