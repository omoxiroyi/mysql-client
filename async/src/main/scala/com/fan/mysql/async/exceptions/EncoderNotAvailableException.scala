

package com.fan.mysql.async.exceptions

import com.fan.mysql.async.db.KindedMessage


class EncoderNotAvailableException(message: KindedMessage)
  extends DatabaseException("Encoder not available for name %s".format(message.kind))
