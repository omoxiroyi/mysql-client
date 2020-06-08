package com.fan.mysql;

import com.fan.mysql.driver.MySQLConnection;
import com.fan.mysql.packet.result.ResultSetPacket;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Main {
    public static void main(String[] args) {
        MySQLConnection client = new MySQLConnection(
                new InetSocketAddress("127.0.0.1", 13307), "root", "199829");

        try {
            client.connect();
            ResultSetPacket rs = client.query("show databases;");
            System.out.println(rs.toString());
            client.quit();
            System.out.println(client.isConnected());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
