/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ai.ntezi.etl.extract;

import com.opencsv.CSVWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author ntezi
 */
public class Extract2 {

    private static Connection connection = null;
//    private static final String dbName = "JMDB2022-10-26auto";
//    private static final String dbURL = "jdbc:derby://localhost:1527/" + dbName + ";create=true";

    private static final String startYear = "2019";
    private static final String endYear = "2019";
    private static final String startMonth = "01-01";
    private static final String endMonth = "12-31";
    private static final String startTime = "00:00:00";
    private static final String endTime = "23:59:59";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        final long startTime = System.nanoTime();
        
        File directoryPath = new File("/Users/ntezi/Dev/DERBY/");
            String dbNames[] = directoryPath.list();
            
            for (String dbName : dbNames) {
                
                File dbDirectory = new File("/Users/ntezi/Dev/DERBY/" + dbName);
                if(dbDirectory.isDirectory() && !dbName.equals("sample")){
                    System.out.println(dbName);
                    connection("jdbc:derby://localhost:1527/" + dbName + ";create=true");
                    retrieve(connection, dbName);
                }

            }
        final long duration = System.nanoTime() - startTime;
        long minutes = TimeUnit.MINUTES.convert(duration, TimeUnit.NANOSECONDS);
        long seconds = TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS);
        long milliSeconds = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
        System.out.println(minutes + " minutes - " + seconds + " seconds - " + milliSeconds + " milli seconds");

    }

    public static void connection(String dbURL) {
        
        try {
            connection = DriverManager.getConnection(dbURL);
            System.out.println("Connection established" + dbURL);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void retrieve(Connection connection, String dbName) {
        ResultSet resultSet = null;
        Statement statement = null;

        String start = startYear + "-" + startMonth + " " + startTime;
        String end = endYear + "-" + endMonth + " " + endTime;

//        String sqlPharmatQuery = "SELECT APP.PRODUCT.CODE, APP.PRODUCT.FAMILLE, APP.PRODUCT.STATUS, APP.INVOICE.ID_INVOICE,\n" +
//"APP.LIST.QUANTITE, APP.LIST.PRICE, APP.LIST.PRIX_REVIENT, APP.LIST.BON_LIVRAISON,\n" +
//"APP.INVOICE.HEURE, APP.LIST.DATE_EXP, APP.INVOICE.NUM_CLIENT, APP.INVOICE.COMPTABILISE,\n" +
//"APP.CREDIT.ID_INVOICE, APP.CLIENT_RAMA.NUM_AFFILIATION, APP.CLIENT_RAMA.PERCENTAGE, APP.CLIENT_RAMA.AGE, APP.CLIENT_RAMA.SEXE, APP.CLIENT_RAMA.LIEN\n" +
//"FROM APP.LIST, APP.INVOICE, APP.PRODUCT, APP.CLIENT_RAMA, APP.CREDIT\n" +
//"WHERE APP.LIST.ID_INVOICE = APP.INVOICE.ID_INVOICE\n" +
//"AND APP.LIST.LIST_ID_PRODUCT = APP.PRODUCT.ID_PRODUCT\n" +
//"AND APP.CREDIT.ID_INVOICE = APP.INVOICE.ID_INVOICE\n" +
//"AND APP.CLIENT_RAMA.NUM_AFFILIATION = APP.CREDIT.NUMERO_AFFILIE\n" +
//"AND APP.INVOICE.NUM_CLIENT = 'RAMA'\n" +
//"AND APP.INVOICE.HEURE > '" + start + "'\n" +
//"AND APP.INVOICE.HEURE < '" + end + "'";
        
        String sqlPharmatQuery1 = "SELECT APP.PRODUCT.CODE, APP.PRODUCT.FAMILLE, APP.PRODUCT.STATUS, APP.PRODUCT.TVA, \n" +
            "APP.INVOICE.ID_INVOICE, APP.INVOICE.HEURE,  APP.INVOICE.NUM_CLIENT, APP.INVOICE.COMPTABILISE, APP.INVOICE.TOTAL, \n" +
            "APP.LIST.QUANTITE, APP.LIST.PRICE, APP.LIST.PRIX_REVIENT, APP.LIST.BON_LIVRAISON, APP.LIST.DATE_EXP,\n" +
            "APP.CREDIT.NUMERO_AFFILIE,\n" +
            "CLIENT.EMPLOYEUR, CLIENT.DATE_EXP AS CLIENT_DATE_EXP, CLIENT.ASSURANCE, CLIENT.EMPLOYEUR, CLIENT.AGE, CLIENT.SEXE, CLIENT.LIEN, CLIENT.PERCENTAGE\n" +
            "FROM APP.INVOICE\n" +
            "LEFT JOIN APP.LIST ON APP.INVOICE.ID_INVOICE = APP.LIST.ID_INVOICE\n" +
            "LEFT JOIN APP.PRODUCT ON APP.LIST.LIST_ID_PRODUCT = APP.PRODUCT.ID_PRODUCT\n" +
            "LEFT JOIN APP.CREDIT ON APP.INVOICE.ID_INVOICE = APP.CREDIT.ID_INVOICE\n" +
            "LEFT JOIN \n" +
            "    (\n" +
            "        SELECT APP.CLIENT.NUM_AFFILIATION, APP.CLIENT.PERCENTAGE, APP.CLIENT.DATE_EXP, APP.CLIENT.ASSURANCE, APP.CLIENT.EMPLOYEUR, APP.CLIENT.AGE, APP.CLIENT.SEXE, APP.CLIENT.LIEN FROM APP.CLIENT \n" +
            "        UNION\n" +
            "        SELECT APP.CLIENT_RAMA.NUM_AFFILIATION, APP.CLIENT_RAMA.PERCENTAGE, APP.CLIENT_RAMA.DATE_EXP, APP.CLIENT_RAMA.ASSURANCE, APP.CLIENT_RAMA.EMPLOYEUR, APP.CLIENT_RAMA.AGE, APP.CLIENT_RAMA.SEXE, APP.CLIENT_RAMA.LIEN FROM APP.CLIENT_RAMA\n" +
            "    ) AS CLIENT ON APP.CREDIT.NUMERO_AFFILIE = CLIENT.NUM_AFFILIATION\n" +
            "\n" +
            "WHERE APP.INVOICE.HEURE > '" + start + "' AND APP.INVOICE.HEURE < '" + end + "'";
        
        String sqlPharmatQuery = "SELECT \n" +
        "    APP.PRODUCT.CODE, \n" +
        "    AVG(APP.LIST.PRIX_REVIENT) AS COST, \n" +
        "    AVG(APP.PRODUCT.PRIX) AS PRICE, \n" +
        "    AVG(APP.PRODUCT.PRIX_RAMA) AS RAMA, \n" +
        "    AVG(APP.PRODUCT.PRIX_SANLAM) AS SANLAM\n" +
        "FROM APP.INVOICE, APP.LIST, APP.PRODUCT \n" +
        "WHERE APP.INVOICE.ID_INVOICE = APP.LIST.ID_INVOICE \n" +
        "AND APP.LIST.LIST_ID_PRODUCT = APP.PRODUCT.ID_PRODUCT\n" +
        "AND APP.INVOICE.HEURE > '" + start + "' \n" +
        "AND APP.INVOICE.HEURE < '" + end + "' \n" +
        "GROUP BY APP.PRODUCT.CODE ";
                try {

            statement = connection.createStatement();
            //  stmt.setFetchSize(100); // 1000, 10
            System.out.println(statement.getFetchSize());  // By default prints 0
            resultSet = statement.executeQuery(sqlPharmatQuery);
            CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter("/Users/ntezi/Dev/PYTHON/SciData/data/extracted/" + dbName + "_from_" + startYear + ".csv")));
            
            System.out.println("**** Started writing Data of " + dbName + " to CSV ****");
            int lines = writer.writeAll(resultSet, true, false, false);
            writer.flush();
            writer.close();
            System.out.println("** OpenCSV -Completed writing the resultSet at " +  new Date() + " Number of lines written to the file " + lines); 
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
