package ai.ntezi.etl.extract;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class DbConnect {

    private final List<String> databases = Arrays.asList("ishyiga", "VINE2022-10-11");

    public Statement[] connect() {
        // Connect to the source databases
        Connection[] sourceConnection = new Connection[databases.size()];
        Statement[] sourceStatement = new Statement[databases.size()];
        for (int i = 0; i < databases.size(); i++) {
            try {
                sourceConnection[i] = DriverManager.getConnection("jdbc:derby://localhost:1527/"+databases.get(i)+";");
                sourceStatement[i] = sourceConnection[i].createStatement();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        return  sourceStatement;
    }

    public ResultSet[] queryExecutor(String sql) {
        Statement[] sourceStatement = connect();
        ResultSet[] sourceResultSet = new ResultSet[databases.size()];
        for (int i = 0; i < databases.size(); i++) {
            try {
                sourceResultSet[i] = sourceStatement[i].executeQuery(sql);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        return sourceResultSet;
    }
}
