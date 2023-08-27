package ai.ntezi.etl.extract;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class Extract {

    private static final List<String> databases = Arrays.asList("ishyiga", "VINE2022-10-11");
    private static final String CLIENTS_SQL = "src/main/resources/sql/extract_clients.sql";
    private static final String CLIENTS_JSON = "clients.json";
    private static final String INVOICES_SQL = "src/main/resources/sql/extract_invoices.sql";
    private static final String INVOICES_JSON = "invoices.json";
    private static final String CONNECTION_URL = "jdbc:derby://localhost:1527/";

    /**
     * Connect to the source databases and return the Statement[] of the source databases to be used to execute sql queries
     *
     * @return Statement[] of the source databases to be used to execute sql queries
     */
    public static Statement[] connect() {
        // Connect to the source databases
        Connection[] sourceConnection = new Connection[databases.size()];
        Statement[] sourceStatement = new Statement[databases.size()];
        for (int i = 0; i < databases.size(); i++) {
            try {
                sourceConnection[i] = DriverManager.getConnection(CONNECTION_URL + databases.get(i) + ";");
                sourceStatement[i] = sourceConnection[i].createStatement();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        return sourceStatement;
    }

    /**
     * Execute sql query on the source databases to extract data from the source databases and write to a json file
     *
     * @param sql sql query to be executed on the source databases to extract data from the source databases and write to a json file
     * @return ResultSet[] of the sql query executed on the source databases to extract data from the source databases and write to a json file
     */
    public static ResultSet[] queryExecutor(String sql) {
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

    /**
     * Read sql file and return the sql query as a string to be executed on the source databases to extract data
     *
     * @param fileName name of the sql file to be read and executed on the source databases to extract data from the source databases and write to a json file
     * @return sql query as a string to be executed on the source databases to extract data from the source databases and write to a json file
     */
    private static String readSqlFile(String fileName) {
        String sql = "";
        try {
            sql = new String(Files.readAllBytes(Paths.get(fileName)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sql;
    }

    /*public void extract_(String sql) {
        ResultSet[] resultSets = queryExecutor(sql);
        for (int i = 0; i < resultSets.length; i++) {
            try {
                ResultSetMetaData metaData = resultSets[i].getMetaData();
                int columnCount = metaData.getColumnCount();
                List<List<String>> rows = new ArrayList<>();
                while (resultSets[i].next()) {
                    List<String> row = new ArrayList<>();
                    for (int j = 1; j <= columnCount; j++) {
                        row.add(resultSets[i].getString(j));
                    }
                    rows.add(row);
                }

                ObjectMapper objectMapper = new ObjectMapper();
                String json = objectMapper.writeValueAsString(rows);
                System.out.println(json);
            } catch (SQLException | JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }*/

    /**
     * Extract data from the source databases and write to a json file
     *
     * @param sql                 sql query to be executed on the source databases to extract data from the source databases and write to a json file
     * @param includeDatabaseName if true, include the database name in the json file as a column name and value for each row of data
     * @param checkDuplicates     if true, check for duplicates before adding to the list of rows to be written to the json file
     * @return json string of the extracted data from the source databases or an empty string if the sql query is empty
     */
    public static String extract(String sql, boolean includeDatabaseName, boolean checkDuplicates) {
        ResultSet[] resultSets = queryExecutor(sql);
        List<Map<String, String>> rows = new ArrayList<>();
        Set<String> uniqueRows = new HashSet<>();
        for (int i = 0; i < resultSets.length; i++) {
            try {
                ResultSetMetaData metaData = resultSets[i].getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSets[i].next()) {
                    Map<String, String> row = new HashMap<>();

                    for (int j = 1; j <= columnCount; j++) {
                        String columnName = metaData.getColumnLabel(j);
                        String columnValue = resultSets[i].getString(j);
                        row.put(columnName, columnValue);

                        // Add database name to the row if includeDatabaseName is true
                        if (includeDatabaseName) {
                            row.put("DATABASE_NAME", databases.get(i));
                        }
                    }

                    // check for duplicates before adding to the list
                    if (checkDuplicates) {
                        String rowHash = Arrays.toString(row.values().toArray());
                        if (!uniqueRows.contains(rowHash)) {
                            uniqueRows.add(rowHash);
                            rows.add(row);
                        }
                    } else {
                        rows.add(row);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Convert the list of rows to JSON string using GSON library
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(rows);
    }

    /**
     * Write the JSON string to a file using FileWriter class from Java IO package
     *
     * @param fileName the name of the file to write to
     * @param json     the JSON string to write to the file
     * @return void
     */
    private static void writeToFile(String fileName, String json) {
        try (FileWriter file = new FileWriter(fileName)) {
            file.write(json);
            System.out.println("Successfully saved JSON file: " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Extract clients from the source databases and save them to a JSON file
     */
    public static void extract_clients() {
        System.out.println("Extracting clients...");
        String sql = readSqlFile(CLIENTS_SQL);
        String json = extract(sql, true, true);
        writeToFile(CLIENTS_JSON, json);
        System.out.println("Done extracting clients.");
    }

    public static void extract_invoices() {
        System.out.println("Extracting invoices...");
        String sql = readSqlFile(INVOICES_SQL);
        String json = extract(sql, true, false);
        writeToFile(INVOICES_JSON, json);
        System.out.println("Done extracting invoices.");
    }

    public static void main(String[] args) {
        extract_clients();
        extract_invoices();
    }
}
