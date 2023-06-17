package com.yeeka.phoenix;

import java.sql.*;

public class TestThickClient {
    public static void main(String[] args) throws SQLException {
        
        // 1 添加连接
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        
        // 2 获取连接
        Connection connection = DriverManager.getConnection(url);
        
        // 3 编译SQL语句
        PreparedStatement statement = connection.prepareStatement("select * from student");

        // 4 执行语句
        ResultSet resultSet = statement.executeQuery();

        // 5 输出结果
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" + resultSet.getString(3));
        }

        // 6 关闭资源
        connection.close();

    }
}











