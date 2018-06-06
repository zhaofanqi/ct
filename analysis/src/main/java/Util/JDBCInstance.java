package Util;

/*
避免开启大量连接
 */

import java.sql.Connection;
import java.sql.SQLException;

public class JDBCInstance {
    private static Connection connection = null;

    private JDBCInstance() {

    }

    public static Connection getInstance() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = JDBCUtils.getConnection();
        }
        return connection;
    }
}
