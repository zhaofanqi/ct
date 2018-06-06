package com.atguigu.obtainId;

import Util.JDBCInstance;
import com.atguigu.mapper.ConsumerMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ConsumerId {
    private static Connection connection = null;
    private static String sql1 = "SELECT `id`  FROM `tb_contacts` WHERE `telephone`=?;";
    private static String sql2 = "INSERT INTO `tb_contacts` VALUES(NULL,?,?);";

    static {
        try {
            connection = JDBCInstance.getInstance();
            sql1 = "SELECT `id`  FROM `tb_contacts` WHERE `telephone`=?;";
            sql2 = "INSERT INTO `tb_contacts` VALUES(NULL,?,?);";
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int getId(ConsumerMapper consumerMapper) {
        String phoneNum = consumerMapper.getPhoneNum();
        String conName = consumerMapper.getConName();
        try {

            // String sql = "select id  from tb_contacts where telephone=?";
//            PreparedStatement preparedStatement = connection.prepareStatement(sql);
//            int i=0;
//            preparedStatement.setString(++i,phoneNum);
//            ResultSet resultSet = preparedStatement.executeQuery();
            /*
            对初次查询结果做分析，若存在则直接返回，若不存在，则需要插入表格并返回插入数据的主键
             */
            int onceResult = onceQuery(phoneNum);
            if (onceResult != 0) return onceResult;

            onceResult = twiceQuery(phoneNum, conName);
            return onceResult;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;

    }

    private int twiceQuery(String phoneNum, String conName) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql2);
        int i = 0;
        preparedStatement.setString(++i, phoneNum);
        preparedStatement.setString(++i, conName);
        preparedStatement.executeUpdate();
        return onceQuery(phoneNum);
    }

    private static int onceQuery(String phoneNum) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql1);
        int i = 0;
        preparedStatement.setString(++i, phoneNum);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.getFetchSize() != 0)
            return resultSet.getInt(i);
        return i;
    }
}
