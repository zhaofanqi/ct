package Util;

import java.sql.*;

public class JDBCUtils {
    private static  String JDBC_MYSQL_DRIVER="com.mysql.jdbc.Driver";
    private static  String JDBC_MYSQL_URL="jdbc:mysql://hadoop102:3306/ct?useUnicode=true&characterEncoding=UTF-8";

    private static  String JDBC_MYSQL_USERNAME="root";
    private static  String JDBC_MYSQL_PASSWORD="123456";

    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        //连接
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        //编译sql
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        //结果集
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Connection getConnection(){
        try {
            Class.forName(JDBC_MYSQL_DRIVER);
            return  DriverManager.getConnection(JDBC_MYSQL_URL, JDBC_MYSQL_USERNAME, JDBC_MYSQL_PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

}
