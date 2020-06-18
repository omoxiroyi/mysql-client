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
    case Success(connection) =>
      connection.dump("5afad620-7695-11e7-a21d-ecf4bbea6648:161928552,b86a3f41-ece0-11e9-8197-246e9615a0d0:1,f01980ee-b0e0-11e9-afc1-c191932e2c51:1-44048")
  }

  /*conn.connect.onComplete {
    case Success(conn) =>
      conn.sendPreparedStatement("select * from user where id = ?", List(1)).onComplete {
        case Success(rs) =>
          println(rs.rows)
      }
  }*/

  Thread.sleep(10000000)
}
