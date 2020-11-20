import com.fan.mysql.async.MySQLConnection;
import com.fan.mysql.async.db.Configuration;
import scala.concurrent.ExecutionContext;

public class JavaMain {
    public static void main(String[] args) {
        ExecutionContext global = scala.concurrent.ExecutionContext.global();

        Configuration conf = new Configuration(
                "root",
                "127.0.0.1",
                13307,
                "199729",
                null
        );

        MySQLConnection connection = new MySQLConnection(conf);

        connection.connect().onComplete(connectionTry -> {
            if (connectionTry.isSuccess()) {
                connectionTry.get().sendQuery("show databases;").onComplete(resultTry -> {
                    if (resultTry.isSuccess()) {
                        System.out.println("result is " + resultTry.get().rows().toString());
                    } else {
                        System.out.println("connection failed");
                    }
                    return null;
                }, global);
            } else {
                System.out.println("connection failed");
            }
            return null;
        }, global);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
