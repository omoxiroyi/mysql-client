import com.fan.mysql.async.MySQLConnection
import com.fan.mysql.async.db.Configuration
import com.fan.mysql.async.util.Log.Logging

object Main extends App with Logging {

  val conf = Configuration(
    "root",
    "127.0.0.1",
    13307,
    Some("199729"))

  val conn = new MySQLConnection(conf)

  conn.connect

  Thread.sleep(10000)
}
