

package com.fan.mysql.async.util

import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object ExecutorServiceUtils {
  implicit val CachedThreadPool: ExecutorService = Executors.newCachedThreadPool(DaemonThreadsFactory("db-async-default"))
  implicit val CachedExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(CachedThreadPool)

  def newFixedPool(count: Int, name: String): ExecutorService = {
    Executors.newFixedThreadPool(count, DaemonThreadsFactory(name))
  }

}
