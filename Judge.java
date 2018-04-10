package com.core;

import com.common.Const;
import com.common.ServerResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Judge {
    public static ServerResponse createDatabase(int id)
    {
        Connection connection = null;
        try {
            Class.forName(Const.JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return ServerResponse.createByErrorMessage(e.getMessage());
        }
        try {
            connection = DriverManager.getConnection(Const.DB_URL, Const.USER, Const.PASS);
            Statement statement = connection.createStatement();
            statement.execute("CREATE DATABASE IF NOT EXISTS user"+id+" DEFAULT CHARSET utf8");
            statement.close();
            connection.close();
            return ServerResponse.createBySuccess();
        } catch (SQLException e) {
            if (connection != null)
                try {
                    connection.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            e.printStackTrace();
            return ServerResponse.createByErrorMessage(e.getMessage());
        }
    }

    public static boolean tableExist(String database,String tableName) {
        String[] sqls = {"use " + database,"SELECT * FROM "+tableName};
        return execSql(sqls);
    }

    public static boolean copyTable(String srcDatabase,String srcTableName,String destDatabase,String destTableName) {
        String[] sqls = {"DROP TABLE IF EXISTS "+destDatabase+"."+destTableName,"CREATE TABLE IF NOT EXISTS "+destDatabase +"."+destTableName +" LIKE "+srcDatabase + "." + srcTableName,
        "INSERT INTO "+destDatabase +"."+destTableName +" SELECT * FROM "+srcDatabase + "." + srcTableName};
        return execSql(sqls);
    }

    private static boolean execSql(String[] sql)
    {
        Connection connection = null;
        try {
            Class.forName(Const.JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        try {
            connection = DriverManager.getConnection(Const.DB_URL, Const.USER, Const.PASS);
            Statement statement = connection.createStatement();
            for (int i = 0; i < sql.length;i++)
                statement.execute(sql[i]);
            statement.close();
            connection.close();
            return true;
        } catch (SQLException e) {
            if (connection != null)
                try {
                    connection.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            e.printStackTrace();
            return false;
        }
    }
}


