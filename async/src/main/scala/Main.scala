import com.fan.mysql.async.MySQLConnection
import com.fan.mysql.async.db.Configuration
import com.fan.mysql.async.util.Log.Logging

import scala.util.Success

object Main extends App with Logging {

  import scala.concurrent.ExecutionContext.Implicits._

  val conf = Configuration(
    "root",
    "127.0.0.1",
    13307,
    Some("199729"),
    Some("test"))

  val conn = new MySQLConnection(conf)

  conn.connect.onComplete {
    case Success(conn) =>
      conn.sendPreparedStatement("select * from user where id = ?", List(1)).onComplete {
        case Success(rs) =>
          println(rs.rows)
      }
  }

  Thread.sleep(10000)
}
