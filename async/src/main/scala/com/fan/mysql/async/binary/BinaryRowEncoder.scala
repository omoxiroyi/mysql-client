package com.fan.mysql.async.binary

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.fan.mysql.async.binary.encoder._
import com.fan.mysql.async.util.Log
import io.netty.buffer.ByteBuf
import org.joda.time._

object BinaryRowEncoder {
  final val log = Log.get[BinaryRowEncoder]
}

class BinaryRowEncoder(charset: Charset) {

  private final val stringEncoder = new StringEncoder(charset)
  private final val encoders = Map[Class[_], BinaryEncoder](
    classOf[String]                                   -> this.stringEncoder,
    classOf[BigInt]                                   -> this.stringEncoder,
    classOf[BigDecimal]                               -> this.stringEncoder,
    classOf[java.math.BigDecimal]                     -> this.stringEncoder,
    classOf[java.math.BigInteger]                     -> this.stringEncoder,
    classOf[Byte]                                     -> ByteEncoder,
    classOf[java.lang.Byte]                           -> ByteEncoder,
    classOf[Short]                                    -> ShortEncoder,
    classOf[java.lang.Short]                          -> ShortEncoder,
    classOf[Int]                                      -> IntegerEncoder,
    classOf[java.lang.Integer]                        -> IntegerEncoder,
    classOf[Long]                                     -> LongEncoder,
    classOf[java.lang.Long]                           -> LongEncoder,
    classOf[Float]                                    -> FloatEncoder,
    classOf[java.lang.Float]                          -> FloatEncoder,
    classOf[Double]                                   -> DoubleEncoder,
    classOf[java.lang.Double]                         -> DoubleEncoder,
    classOf[LocalDateTime]                            -> LocalDateTimeEncoder,
    classOf[DateTime]                                 -> DateTimeEncoder,
    classOf[LocalDate]                                -> LocalDateEncoder,
    classOf[java.util.Date]                           -> JavaDateEncoder,
    classOf[java.sql.Timestamp]                       -> SQLTimestampEncoder,
    classOf[java.sql.Date]                            -> SQLDateEncoder,
    classOf[java.sql.Time]                            -> SQLTimeEncoder,
    classOf[scala.concurrent.duration.FiniteDuration] -> DurationEncoder,
    classOf[Array[Byte]]                              -> ByteArrayEncoder,
    classOf[Boolean]                                  -> BooleanEncoder,
    classOf[java.lang.Boolean]                        -> BooleanEncoder
  )

  def encoderFor(v: Any): BinaryEncoder = {

    this.encoders.get(v.getClass) match {
      case Some(encoder) => encoder
      case None =>
        v match {
          case _: CharSequence                       => this.stringEncoder
          case _: BigInt                             => this.stringEncoder
          case _: java.math.BigInteger               => this.stringEncoder
          case _: BigDecimal                         => this.stringEncoder
          case _: java.math.BigDecimal               => this.stringEncoder
          case _: ReadableDateTime                   => DateTimeEncoder
          case _: ReadableInstant                    => ReadableInstantEncoder
          case _: LocalDateTime                      => LocalDateTimeEncoder
          case _: java.sql.Timestamp                 => SQLTimestampEncoder
          case _: java.sql.Date                      => SQLDateEncoder
          case _: java.util.Calendar                 => CalendarEncoder
          case _: LocalDate                          => LocalDateEncoder
          case _: LocalTime                          => LocalTimeEncoder
          case _: java.sql.Time                      => SQLTimeEncoder
          case _: scala.concurrent.duration.Duration => DurationEncoder
          case _: java.util.Date                     => JavaDateEncoder
          case _: ByteBuffer                         => ByteBufferEncoder
          case _: ByteBuf                            => ByteBufEncoder
        }
    }

  }

}
