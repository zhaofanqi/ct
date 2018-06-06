package com.atguigu.obtainId;

import Util.JDBCInstance;
import com.atguigu.mapper.DateMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 日期维度id 获取
 *
 * @author zhaofanqi
 */
public class DateId {

    /**
     * mysql连接
     */
    private static Connection connection = null;

    /**
     * 查询时间维度 id
     */
    private static PreparedStatement query;

    /**
     * 插入时间维度
     */
    private static PreparedStatement insert;

    static {
        try {
            connection = JDBCInstance.getInstance();
            query = connection.prepareStatement("SELECT `id`  FROM `tb_dimension_date` WHERE `year`=? AND `month`=? AND `day`=?;");
            insert = connection.prepareStatement("INSERT INTO  `tb_dimension_date`  VALUES(NULL,?,?,?);");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取时间维度id
     *
     * @param dateMapper
     * @return
     */
    public static int getId(DateMapper dateMapper) {
        String year = dateMapper.getYear();
        String month = dateMapper.getMonth();
        String day = dateMapper.getDay();
        try {
            int idDateDimension = queryId(year, month, day);
            if (idDateDimension != 0) {
                return idDateDimension;
            }

            return insertAndQueryId(year, month, day);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.err.println("com.atguigu.obtainId.DateId.getId 获取id失败");
        return 0;
    }

    /**
     * 插入并返回id
     *
     * @param year
     * @param month
     * @param day
     * @return
     * @throws SQLException
     */
    private static int insertAndQueryId(String year, String month, String day) throws SQLException {
        int i = 0;
        insert.setString(++i, year);
        insert.setString(++i, month);
        insert.setString(++i, day);
        insert.executeUpdate();

        return queryId(year, month, day);
    }

    /**
     * 查询维度id
     *
     * @param year
     * @param month
     * @param day
     * @return
     * @throws SQLException
     */
    private static int queryId(String year, String month, String day) throws SQLException {
        int i = 0;
        query.setString(++i, year);
        query.setString(++i, month);
        query.setString(++i, day);

        ResultSet resultSet = query.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
    }
}
