package com.fan.mysql.async.util

import org.slf4j.{Logger, LoggerFactory}

object Log {

  def get[T](implicit tag: reflect.ClassTag[T]): Logger = {
    LoggerFactory.getLogger(tag.runtimeClass.getName)
  }

  def getByName(name: String): Logger = {
    LoggerFactory.getLogger(name)
  }

  trait Logging {
    protected val logger: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
  }

}
