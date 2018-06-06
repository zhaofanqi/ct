package com.atguigu.obtainId;

import Util.JDBCInstance;
import com.atguigu.mapper.DateMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DateId {
    private static Connection connection = null;
    private static String sql1 = "SELECT `id`  FROM `tb_dimension_date` WHERE `year`=? AND `month`=? AND `day`=?;";
    private static String sql2 = "INSERT INTO  `tb_dimension_date`  VALUES(NULL,?,?,?);";

    static {
        try {
            connection = JDBCInstance.getInstance();
            sql1 = "SELECT `id`  FROM `tb_dimension_date` WHERE `year`=? AND `month`=? AND `day`=?;";
            sql2 = "INSERT INTO  `tb_dimension_date`  VALUES(NULL,?,?,?);";
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static int getId(DateMapper dateMapper) {
        String year = dateMapper.getYear();
        String month = dateMapper.getMonth();
        String day = dateMapper.getDay();
        try {
            int idDateDimension = onceQuery(year, month, day);
            if (idDateDimension != 0){

            }

            idDateDimension = twiceQuery(year, month, day);
            return idDateDimension;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("com.atguigu.obtainId.DateId 获取id失败");
        return 0;
    }

    private static int twiceQuery(String year, String month, String day) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql2);
        int i = 0;
        preparedStatement.setString(++i, year);
        preparedStatement.setString(++i, month);
        preparedStatement.setString(++i, day);

        preparedStatement.executeUpdate();

        return onceQuery(year, month, day);

    }

    private static int onceQuery(String year, String month, String day) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql1);
        int i = 0;
        preparedStatement.setString(++i, year);
        preparedStatement.setString(++i, month);
        preparedStatement.setString(++i, day);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.getFetchSize() != 0)
            return resultSet.getInt(i);
        return i;
    }
}
