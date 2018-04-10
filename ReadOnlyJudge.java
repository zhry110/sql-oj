package com.core;



import com.common.Const;
import com.common.ServerResponse;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

public class ReadOnlyJudge {
    private String user,passwd = "";
    private Connection connection = null;
    public ReadOnlyJudge() throws ClassNotFoundException, SQLException {
        user = "readonly";
        Class.forName(Const.JDBC_DRIVER);
        connection = DriverManager.getConnection(Const.DB_URL, user, passwd);
    }
    private void closeResource(Statement statement1,Statement statement2,ResultSet resultSet1,ResultSet resultSet2)
    {
        try {
            if (statement1 != null)
                statement1.close();
            if (statement2 != null)
                statement2.close();
            if (resultSet1 != null)
                resultSet1.close();
            if (resultSet2 != null)
                resultSet2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    public ServerResponse doJudge(String sql, String answer,String database)
    {
        Statement userStmt = null,answerStmt = null;
        try {
            userStmt = connection.createStatement();
            answerStmt = connection.createStatement();
            answerStmt.execute("use " + database);//use database
            ResultSet userResult = null,answerResult = null;
            userResult = userStmt.executeQuery(sql);
            answerResult = answerStmt.executeQuery(answer);
            ResultSetMetaData userMetaData = userResult.getMetaData();
            ResultSetMetaData answerMetaData = answerResult.getMetaData();
            int userColumnCount = userMetaData.getColumnCount();
            int answerColumnCount = answerMetaData.getColumnCount();
            if (userColumnCount != answerColumnCount) {
                closeResource(userStmt,answerStmt,userResult,answerResult);
                return ServerResponse.createByErrorMessage("结果集列数不正确");
            }
            InputStream userInputStream,answerInputStream;
            int charTemp;
            for (int i = 0; i < userColumnCount;i++)
            {
                if (userMetaData.getColumnType(i + 1) != answerMetaData.getColumnType(i + 1)) {
                    closeResource(userStmt,answerStmt,userResult,answerResult);
                    return ServerResponse.createByErrorMessage("结果集列类型不正确");
                }
            }
            while (userResult.next() && answerResult.next())
            {
                for (int i = 0; i < userColumnCount;i++)
                {
                    userInputStream = userResult.getBinaryStream(i + 1);
                    answerInputStream = answerResult.getBinaryStream(i + 1);
                    try {
                        while (true)
                        {
                            if ((charTemp = userInputStream.read()) == answerInputStream.read())
                            {
                                if (charTemp == -1)
                                    break;
                                continue;
                            } else {
                                closeResource(userStmt,answerStmt,userResult,answerResult);
                                return ServerResponse.createByErrorMessage("结果集错误");
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        closeResource(userStmt,answerStmt,userResult,answerResult);
                        return ServerResponse.createByErrorMessage(e.getMessage());
                    }
                }
            }
            userResult.last();
            answerResult.last();
            if (userResult.getRow() != answerResult.getRow()) {
                closeResource(userStmt,answerStmt,userResult,answerResult);
                return ServerResponse.createByErrorMessage("结果集过多或者过少");
            }
            closeResource(userStmt,answerStmt,userResult,answerResult);
            return ServerResponse.createBySuccessMessage("Accept");
        } catch (SQLException e) {
            e.printStackTrace();
            closeResource(userStmt,answerStmt,null,null);
            return ServerResponse.createByErrorMessage(e.getMessage());
        }

    }

}
