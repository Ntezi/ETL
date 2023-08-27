/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ai.ntezi.etl.extract;

import java.beans.Statement;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author ntezi
 */
public class Extract1 {

    private static final String dbName = "ishyiga";
    private static final String dbURL = "jdbc:derby://localhost:1527/" + dbName + ";create=true";
    private static final String driver = "org.apache.derby.jdbc.ClientDriver";

    private static Connection connection = null;
    private static Statement statement = null;
    private static ResultSet resultSet = null;

    private static final String year = "2019";
    private static final String startMonth = "01-01";
    private static final String endMonth = "12-31";
    private static final String startTime = "00:00:00";
    private static final String endTime = "23:59:59";

    public static void main(String[] args) {
        final long startTime = System.nanoTime();
        createConnection();
        preprocess();
        shutdown();
        final long duration = System.nanoTime() - startTime;
        long minutes = TimeUnit.MINUTES.convert(duration, TimeUnit.NANOSECONDS);
        long seconds = TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS);
        long milliSeconds = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
        System.out.println(minutes + " minutes - " + seconds + " seconds - " + milliSeconds + " milli seconds");
    }

    private static void createConnection() {
        try {
            //Get a connection
            connection = DriverManager.getConnection(dbURL);
            if (connection != null) {
                System.out.println("Connected to database: " + dbName);
                System.out.println(dbURL);
            }
        } catch (Exception except) {
            except.printStackTrace();
        }
    }

    private static void preprocess() {
        String query = "pharma";
        String start = year + "-" + startMonth + " " + startTime;
        String end = year + "-" + endMonth + " " + endTime;

        String sqlPharmatQuery = "SELECT APP.PRODUCT.CODE, APP.PRODUCT.FAMILLE, APP.PRODUCT.STATUS, APP.INVOICE.ID_INVOICE, "
                + "APP.LIST.QUANTITE, APP.LIST.PRICE, APP.LIST.PRIX_REVIENT, APP.LIST.BON_LIVRAISON, "
                + "APP.INVOICE.HEURE, APP.LIST.DATE_EXP, APP.INVOICE.NUM_CLIENT, APP.INVOICE.COMPTABILISE "
                + "FROM APP.LIST, APP.INVOICE, APP.PRODUCT "
                + "WHERE APP.LIST.ID_INVOICE = APP.INVOICE.ID_INVOICE "
                + "AND APP.LIST.LIST_ID_PRODUCT = APP.PRODUCT.ID_PRODUCT "
                + "AND APP.INVOICE.HEURE > '" + start + "' "
                + "AND APP.INVOICE.HEURE < '" + end + "'";

        String sqlImportQuery = "SELECT APP.PRODUCT.CODE, APP.PRODUCT.FAMILLE, APP.PRODUCT.STATUS, APP.INVOICE.ID_INVOICE, "
                + "APP.LIST.QUANTITE, APP.LIST.PRICE, APP.LIST.PRIX_REVIENT, APP.LIST.BON_LIVRAISON, APP.INVOICE.HEURE, "
                + "APP.LIST.DATE_EXP, APP.INVOICE.RETOUR, "
                + "APP.INVOICE.NUM_FACT, APP.INVOICE.TARIF, APP.INVOICE.STATUS AS INVOICE_STATUS "
                + "FROM APP.LIST, APP.INVOICE, APP.PRODUCT "
                + "WHERE APP.LIST.ID_INVOICE = APP.INVOICE.ID_INVOICE "
                + "AND APP.LIST.LIST_ID_PRODUCT = APP.PRODUCT.ID_PRODUCT "
                + "AND APP.INVOICE.HEURE > '" + start + "'"
                + "AND APP.INVOICE.HEURE < '" + end + "'";

        System.out.print(sqlPharmatQuery);
        System.out.print("\n");

        try {

           
            PreparedStatement statement = connection.prepareStatement(sqlPharmatQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            statement.setFetchSize(Integer.MIN_VALUE);
            resultSet = statement.executeQuery();

//            statement = (Statement) connection.createStatement();
//            ResultSet results = statement.executeQuery(sqlPharmatQuery);
            String path = "/Users/ntezi/Dev/PYTHON/SciData/data/" + dbName + "_" + year + "-" + startMonth + "_" + year + "-" + endMonth + ".csv";
            File file = new File(path);
            FileWriter writer = new FileWriter(file, true);  //True = Append to file, false = Overwrite

            if (resultSet != null) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int numberCols = resultSetMetaData.getColumnCount();
                String columnName;
                for (int i = 1; i <= numberCols; i++) {
                    columnName = resultSetMetaData.getColumnLabel(i);
                    writer.write(columnName + ",");
                }
                writer.write("\r\n");
                while ((resultSet.next())) {
                    writer.write(resultSet.getString("CODE").replace(',', ' '));
                    writer.write(",");
                    writer.write(resultSet.getString("FAMILLE"));
                    writer.write(",");
                    writer.write(resultSet.getString("STATUS"));
                    writer.write(",");
                    writer.write(resultSet.getString("ID_INVOICE"));
                    writer.write(",");
                    writer.write(resultSet.getString("QUANTITE"));
                    writer.write(",");
                    writer.write(resultSet.getString("PRICE"));
                    writer.write(",");
                    writer.write(resultSet.getString("PRIX_REVIENT"));
                    writer.write(",");
                    writer.write(resultSet.getString("BON_LIVRAISON"));
                    writer.write(",");
                    writer.write(resultSet.getString("HEURE"));
                    writer.write(",");
                    writer.write(resultSet.getString("DATE_EXP"));
                    writer.write(",");
                    writer.write(resultSet.getString("NUM_CLIENT").replace(',', ' '));
                    writer.write(",");
                    writer.write(resultSet.getString("COMPTABILISE"));
                    /*if (query.equals("import")) {
                        writer.write(",");
                        writer.write(resultSet.getString("RETOUR"));
                        writer.write(",");
                        writer.write(resultSet.getString("NUM_FACT"));
                        writer.write(",");
                        writer.write(resultSet.getString("TARIF"));
                        writer.write(",");
                        writer.write(resultSet.getString("INVOICE_STATUS"));
                    }*/
                    writer.write("\r\n");
                }
                writer.close();
                System.out.println("Write success!");

                resultSet.close();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void shutdown() {
        try {
            if (statement != null) {
                resultSet.close();
            }
            if (connection != null) {
                DriverManager.getConnection(dbURL + ";shutdown=true");
                connection.close();
            }
        } catch (SQLException ignored) {

        }

    }
}
