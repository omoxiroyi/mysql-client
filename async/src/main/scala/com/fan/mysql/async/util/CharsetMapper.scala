

package com.fan.mysql.async.util


import java.nio.charset.Charset

import com.fan.mysql.async.exceptions.CharsetMappingNotAvailableException
import io.netty.util.CharsetUtil

object CharsetMapper {

  final val Binary = 63

  final val DefaultCharsetsByCharset = Map[Charset, Int](
    CharsetUtil.UTF_8 -> 83,
    CharsetUtil.US_ASCII -> 11,
    CharsetUtil.US_ASCII -> 65,
    CharsetUtil.ISO_8859_1 -> 3,
    CharsetUtil.ISO_8859_1 -> 69
  )

  final val DefaultCharsetsById = DefaultCharsetsByCharset.map { pair => (pair._2, pair._1.name()) }

  final val Instance = new CharsetMapper()
}

class CharsetMapper(charsetsToIntComplement: Map[Charset, Int] = Map.empty[Charset, Int]) {

  private var charsetsToInt = CharsetMapper.DefaultCharsetsByCharset ++ charsetsToIntComplement

  def toInt(charset: Charset): Int = {
    charsetsToInt.getOrElse(charset, {
      throw new CharsetMappingNotAvailableException(charset)
    })
  }

}