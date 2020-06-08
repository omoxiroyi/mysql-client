package com.fan.mysql.util

import org.slf4j.{LoggerFactory, Marker, Logger => Slf4jLogger}

/**
 * Typical logger interface.
 */
trait LoggerLike {
  /**
   * The underlying SLF4J Logger.
   */
  def logger: Slf4jLogger

  /**
   * The underlying SLF4J Logger.
   */
  lazy val underlyingLogger: Slf4jLogger = logger

  @inline def enabled: Boolean = true

  /**
   * `true` if the logger instance is enabled for the `TRACE` level.
   */
  def isTraceEnabled(implicit mc: MarkerContext): Boolean =
    enabled && mc.marker.fold(logger.isTraceEnabled)(logger.isTraceEnabled)

  /**
   * `true` if the logger instance is enabled for the `DEBUG` level.
   */
  def isDebugEnabled(implicit mc: MarkerContext): Boolean =
    enabled && mc.marker.fold(logger.isDebugEnabled)(logger.isDebugEnabled)

  /**
   * `true` if the logger instance is enabled for the `INFO` level.
   */
  def isInfoEnabled(implicit mc: MarkerContext): Boolean =
    enabled && mc.marker.fold(logger.isInfoEnabled)(logger.isInfoEnabled)

  /**
   * `true` if the logger instance is enabled for the `WARN` level.
   */
  def isWarnEnabled(implicit mc: MarkerContext): Boolean =
    enabled && mc.marker.fold(logger.isWarnEnabled)(logger.isWarnEnabled)

  /**
   * `true` if the logger instance is enabled for the `ERROR` level.
   */
  def isErrorEnabled(implicit mc: MarkerContext): Boolean =
    enabled && mc.marker.fold(logger.isErrorEnabled)(logger.isErrorEnabled)

  /**
   * Logs a message with the `TRACE` level.
   *
   * @param message the message to log
   * @param mc      the implicit marker context, if defined.
   */
  def trace(message: => String)(implicit mc: MarkerContext): Unit = {
    if (isTraceEnabled) {
      mc.marker match {
        case None => logger.trace(message)
        case Some(marker) => logger.trace(marker, message)
      }
    }
  }

  /**
   * Logs a message with the `TRACE` level.
   *
   * @param message the message to log
   * @param error   the associated exception
   * @param mc      the implicit marker context, if defined.
   */
  def trace(message: => String, error: => Throwable)(implicit mc: MarkerContext): Unit = {
    if (isTraceEnabled) {
      mc.marker match {
        case None => logger.trace(message, error)
        case Some(marker) => logger.trace(marker, message, error)
      }
    }
  }

  /**
   * Logs a message with the `DEBUG` level.
   *
   * @param message the message to log
   * @param mc      the implicit marker context, if defined.
   */
  def debug(message: => String)(implicit mc: MarkerContext): Unit = {
    if (isDebugEnabled) {
      mc.marker match {
        case None => logger.debug(message)
        case Some(marker) => logger.debug(marker, message)
      }
    }
  }

  /**
   * Logs a message with the `DEBUG` level.
   *
   * @param message the message to log
   * @param error   the associated exception
   * @param mc      the implicit marker context, if defined.
   */
  def debug(message: => String, error: => Throwable)(implicit mc: MarkerContext): Unit = {
    if (isDebugEnabled) {
      mc.marker match {
        case None => logger.debug(message, error)
        case Some(marker) => logger.debug(marker, message, error)
      }
    }
  }

  /**
   * Logs a message with the `INFO` level.
   *
   * @param message the message to log
   * @param mc      the implicit marker context, if defined.
   */
  def info(message: => String)(implicit mc: MarkerContext): Unit = {
    if (isInfoEnabled) {
      mc.marker match {
        case None => logger.info(message)
        case Some(marker) => logger.info(marker, message)
      }
    }
  }

  /**
   * Logs a message with the `INFO` level.
   *
   * @param message the message to log
   * @param error   the associated exception
   * @param mc      the implicit marker context, if defined.
   */
  def info(message: => String, error: => Throwable)(implicit mc: MarkerContext): Unit = {
    if (isInfoEnabled) {
      mc.marker match {
        case None => logger.info(message, error)
        case Some(marker) => logger.info(marker, message, error)
      }
    }
  }

  /**
   * Logs a message with the `WARN` level.
   *
   * @param message the message to log
   * @param mc      the implicit marker context, if defined.
   */
  def warn(message: => String)(implicit mc: MarkerContext): Unit = {
    if (isWarnEnabled) {
      mc.marker match {
        case None => logger.warn(message)
        case Some(marker) => logger.warn(marker, message)
      }
    }
  }

  /**
   * Logs a message with the `WARN` level.
   *
   * @param message the message to log
   * @param error   the associated exception
   * @param mc      the implicit marker context, if defined.
   */
  def warn(message: => String, error: => Throwable)(implicit mc: MarkerContext): Unit = {
    if (isWarnEnabled) {
      mc.marker match {
        case None => logger.warn(message, error)
        case Some(marker) => logger.warn(marker, message, error)
      }
    }
  }

  /**
   * Logs a message with the `ERROR` level.
   *
   * @param message the message to log
   * @param mc      the implicit marker context, if defined.
   */
  def error(message: => String)(implicit mc: MarkerContext): Unit = {
    if (isErrorEnabled) {
      mc.marker match {
        case None => logger.error(message)
        case Some(marker) => logger.error(marker, message)
      }
    }
  }

  /**
   * Logs a message with the `ERROR` level.
   *
   * @param message the message to log
   * @param error   the associated exception
   * @param mc      the implicit marker context, if defined.
   */
  def error(message: => String, error: => Throwable)(implicit mc: MarkerContext): Unit = {
    if (isErrorEnabled) {
      mc.marker match {
        case None => logger.error(message, error)
        case Some(marker) => logger.error(marker, message, error)
      }
    }
  }
}

/**
 * A trait that can mixed into a class or trait to add a `logger` named based on the class name.
 */
trait Logging {
  protected val logger: Logger = Logger(getClass)
}

/**
 * A Normal logger.
 *
 * @param logger the underlying SL4FJ logger
 */
class Logger private(val logger: Slf4jLogger, isEnabled: => Boolean) extends LoggerLike {
  def this(logger: Slf4jLogger) = this(logger, true)

  @inline override def enabled: Boolean = isEnabled
}

/**
 * High-level API for logging operations.
 *
 * For example, logging with the default application logger:
 * {{{
 * Logger.info("Hello!")
 * }}}
 *
 * Logging with a custom logger:
 * {{{
 * Logger("my.logger").info("Hello!")
 * }}}
 */
object Logger {
  /**
   * Obtains a logger instance.
   *
   * @param name the name of the logger
   * @return a logger
   */
  def apply(name: String): Logger = new Logger(LoggerFactory.getLogger(name))

  /**
   * Obtains a logger instance.
   *
   * @param clazz a class whose name will be used as logger name
   * @return a logger
   */
  def apply(clazz: Class[_]): Logger = new Logger(LoggerFactory.getLogger(clazz.getName.stripSuffix("$")))
}

/**
 * A MarkerContext trait, to provide easy access to org.slf4j.Marker in Logger API.  This is usually accessed
 * with a marker through an implicit conversion from a Marker.
 *
 * {{{
 *   implicit val markerContext: MarkerContext = org.slf4j.MarkerFactory.getMarker("EXAMPLEMARKER")
 *   log.error("This message will be logged with the EXAMPLEMARKER marker")
 * }}}
 *
 */
trait MarkerContext {
  /**
   * @return an SLF4J marker, if one has been defined.
   */
  def marker: Option[Marker]
}

object MarkerContext extends LowPriorityMarkerContextImplicits {
  /**
   * Provides an instance of a MarkerContext from a Marker.  The explicit form is useful when
   * you want to explicitly tag a log message with a particular Marker and you already have a
   * Marker in implicit scope.
   *
   * {{{
   *   implicit val implicitContext: MarkerContext = ...
   *   val explicitContext: MarkerContext = MarkerContext(MarkerFactory.getMarker("EXPLICITMARKER"))
   *
   *   // do not use the implicit MarkerContext
   *   log.error("This message is logged with EXPLICITMARKER")(explicitContext)
   * }}}
   *
   * @param marker the marker to wrap in DefaultMarkerContext
   * @return an instance of DefaultMarkerContext.
   */
  def apply(marker: Marker): MarkerContext = {
    new DefaultMarkerContext(marker)
  }
}

trait LowPriorityMarkerContextImplicits {
  /**
   * A MarkerContext that returns None.  This is used as the "default" marker context if
   * no implicit MarkerContext is found in local scope (meaning there is nothing defined
   * through import or "implicit val").
   */
  implicit val NoMarker: MarkerContext = MarkerContext(null)

  /**
   * Enables conversion from a marker to a MarkerContext:
   *
   * {{{
   *  val mc: MarkerContext = MarkerFactory.getMarker("SOMEMARKER")
   * }}}
   *
   * @param marker the SLF4J marker to convert
   * @return the result of `MarkerContext.apply(marker)`
   */
  implicit def markerToMarkerContext(marker: Marker): MarkerContext = {
    MarkerContext(marker)
  }
}

/**
 * A default marker context.  This is used by `MarkerContext.apply`, but can also be used to provide
 * explicit typing for markers.  For example, to define a SecurityContext marker, you can define a case
 * object extending DefaultMarkerContext:
 *
 * {{{
 * case object SecurityMarkerContext extends DefaultMarkerContext(MarkerFactory.getMarker("SECURITY"))
 * }}}
 *
 * @param someMarker a marker used in the `marker` method.
 */
class DefaultMarkerContext(someMarker: Marker) extends MarkerContext {
  def marker: Option[Marker] = Option(someMarker)
}

object MarkerContexts {

  case object SecurityMarkerContext extends DefaultMarkerContext(org.slf4j.MarkerFactory.getMarker("SECURITY"))

}