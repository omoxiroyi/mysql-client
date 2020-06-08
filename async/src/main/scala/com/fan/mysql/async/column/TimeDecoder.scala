

package com.fan.mysql.async.column


import scala.concurrent.duration._

object TimeDecoder extends ColumnDecoder {

  final val Hour = 1.hour.toMillis

  override def decode(value: String): Duration = {

    val pieces = value.split(':')

    val secondsAndMillis = pieces(2).split('.')

    val parts = if (secondsAndMillis.length == 2) {
      (secondsAndMillis(0).toInt, secondsAndMillis(1).toInt)
    } else {
      (secondsAndMillis(0).toInt, 0)
    }

    val hours = pieces(0).toInt
    val minutes = pieces(1).toInt

    hours.hours + minutes.minutes + parts._1.seconds + parts._2.millis
  }

}