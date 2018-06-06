package Util;

import javax.annotation.concurrent.ThreadSafe;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 获取连接单例
 *
 * 作用：避免开启大量连接
 */
@ThreadSafe
public class JDBCInstance {
    private static Connection connection = null;

    private JDBCInstance() {
    }

    public static synchronized Connection getInstance() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = JDBCUtils.getConnection();
        }
        return connection;
    }
}
