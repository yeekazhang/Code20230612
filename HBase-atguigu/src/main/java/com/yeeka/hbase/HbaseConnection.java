package com.yeeka.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnection {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

//        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        conf.addResource("hbase-site.xml");

        Connection connection = ConnectionFactory.createConnection(conf);

        System.out.println(connection);
    }
}
