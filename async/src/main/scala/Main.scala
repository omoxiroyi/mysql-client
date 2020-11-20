import com.fan.mysql.async.MySQLConnection
import com.fan.mysql.async.db.Configuration
import com.fan.mysql.async.util.Log.Logging

import scala.concurrent.ExecutionContext.global
import scala.util.Success

object Main extends App with Logging {

  val conf = Configuration("root", "127.0.0.1", 13307, Some("199729"), Some("test"))

  val conn = new MySQLConnection(conf)

  /*conn.connect.onComplete {
    case Success(connection) =>
      connection.dump("mysql-bin.000243:154",
        null,
        event => {
          logger.info(s"receive a binlog event.\n$event")
          event.getOriginData
          true
        }, global.asInstanceOf[Executor])
        .onComplete {
          case Failure(exception) =>
            logger.info("Start dump binlog failed", exception)
          case _ =>
            logger.info("Start dump binlog")
        }
  }*/

  conn.connect.onComplete {
    case Success(conn) =>
      1 to 1 foreach { v =>
        conn
          .sendPreparedStatement("select * from user where time > ?", List("'2020-09-29 11:01:45'"))
          .onComplete {
            case Success(rs) =>
              println(rs.rows)
          }(global)
      }
  }(global)

  Thread.sleep(10000000)
}
