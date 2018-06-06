package com.atguigu.obtainId;

import Util.JDBCInstance;
import com.atguigu.mapper.ConsumerMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 联系人维度
 *
 * @author zhaofanqi
 */
public class ConsumerId {

    private static Connection connection = null;

    private static PreparedStatement query;

    private static PreparedStatement insert;

    static {
        try {
            connection = JDBCInstance.getInstance();

            query = connection.prepareStatement("SELECT `id`  FROM `tb_contacts` WHERE `telephone`=?;");
            insert = connection.prepareStatement("INSERT INTO `tb_contacts` VALUES(NULL,?,?);");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取联系人维度id
     *
     * @param consumerMapper
     * @return
     * @throws SQLException
     */
    public static int getId(ConsumerMapper consumerMapper) {
        try {
            String phoneNum = consumerMapper.getPhoneNum();
            int onceResult = queryId(phoneNum);
            if (onceResult != 0) {
                return onceResult;
            }

            return insertAndQueryId(phoneNum, consumerMapper.getConName());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.err.println("com.atguigu.obtainId.ConsumerId.getId: 获取联系人维度id获取为空");
        return 0;
    }

    /**
     * 插入联系人并查询id
     *
     * @param phoneNum
     * @param conName
     * @return
     * @throws SQLException
     */
    private static int insertAndQueryId(String phoneNum, String conName) throws SQLException {
        // 插入
        int i = 0;
        insert.setString(++i, phoneNum);
        insert.setString(++i, conName);
        insert.executeUpdate();

        // 查询
        return queryId(phoneNum);
    }

    /**
     * 查询联系人id
     *
     * @param phoneNum
     * @return
     * @throws SQLException
     */
    private static int queryId(String phoneNum) throws SQLException {
        int i = 0;
        query.setString(++i, phoneNum);
        ResultSet resultSet = query.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
    }
}
