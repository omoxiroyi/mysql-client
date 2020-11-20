package com.fan.mysql.async.pool

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledFuture, TimeUnit, TimeoutException}

import io.netty.channel.EventLoopGroup

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Promise}

trait TimeoutScheduler {

  private val isTimeoutRef = new AtomicBoolean(false)

  /** The event loop group to be used for scheduling.
    *
    * @return
    */
  def eventLoopGroup: EventLoopGroup

  /** Implementors should decide here what they want to do when a timeout occur
    */
  def onTimeout() // implementors should decide here what they want to do when a timeout occur

  /** We need this property as isClosed takes time to complete and
    * we don't want the connection to be used again.
    *
    * @return
    */
  def isTimeOuted: Boolean =
    isTimeoutRef.get

  def addTimeout[A](promise: Promise[A], durationOption: Option[Duration])(implicit
      executionContext: ExecutionContext
  ): Option[ScheduledFuture[_]] = {
    durationOption.map { duration =>
      val scheduledFuture = schedule(
        {
          if (
            promise.tryFailure(
              new TimeoutException(
                s"Operation is timeout after it took too long to return ($duration)"
              )
            )
          ) {
            isTimeoutRef.set(true)
            onTimeout()
          }
        },
        duration
      )
      promise.future.onComplete(_ => scheduledFuture.cancel(false))

      scheduledFuture
    }
  }

  def schedule(block: => Unit, duration: Duration): ScheduledFuture[_] =
    eventLoopGroup.schedule(
      new Runnable {
        override def run(): Unit = block
      },
      duration.toMillis,
      TimeUnit.MILLISECONDS
    )

  def scheduleAtFixedRate(block: => Unit, duration: Duration): ScheduledFuture[_] =
    eventLoopGroup.scheduleAtFixedRate(
      () => block,
      duration.toMillis,
      duration.toMillis,
      TimeUnit.MILLISECONDS
    )
}
