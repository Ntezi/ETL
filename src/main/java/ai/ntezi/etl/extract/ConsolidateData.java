/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package ai.ntezi.etl.extract;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author ntezi
 */
public class ConsolidateData {

    public static void main(String[] args) throws FileNotFoundException {
        
        String sqlQuery = " SELECT \n" +
"    APP.PRODUCT.CODE, \n" +
"    APP.PRODUCT.FAMILLE, \n" +
"    APP.PRODUCT.STATUS, \n" +
"    APP.PRODUCT.TVA,  \n" +
"    APP.INVOICE.ID_INVOICE, \n" +
"    APP.INVOICE.HEURE,  \n" +
"    APP.INVOICE.NUM_CLIENT, \n" +
"    APP.INVOICE.COMPTABILISE, \n" +
"    APP.INVOICE.TOTAL,  \n" +
"    APP.LIST.QUANTITE, \n" +
"    APP.LIST.PRICE, \n" +
"    APP.LIST.PRIX_REVIENT, \n" +
"    APP.LIST.BON_LIVRAISON, \n" +
"    APP.LIST.DATE_EXP, \n" +
"    APP.CREDIT.NUMERO_AFFILIE, \n" +
"    CLIENT.EMPLOYEUR, \n" +
"    CLIENT.DATE_EXP AS CLIENT_DATE_EXP, \n" +
"    CLIENT.ASSURANCE, \n" +
"    CLIENT.EMPLOYEUR, \n" +
"    CLIENT.AGE, \n" +
"    CLIENT.SEXE, \n" +
"    CLIENT.LIEN, \n" +
"    CLIENT.PERCENTAGE \n" +
"FROM \n" +
"    APP.INVOICE \n" +
"INNER JOIN \n" +
"    APP.LIST ON APP.INVOICE.ID_INVOICE = APP.LIST.ID_INVOICE \n" +
"INNER JOIN \n" +
"    APP.PRODUCT ON APP.LIST.LIST_ID_PRODUCT = APP.PRODUCT.ID_PRODUCT \n" +
"INNER JOIN \n" +
"    APP.CREDIT ON APP.INVOICE.ID_INVOICE = APP.CREDIT.ID_INVOICE \n" +
"INNER JOIN \n" +
"    ( \n" +
"        SELECT NUM_AFFILIATION, PERCENTAGE, DATE_EXP, ASSURANCE, EMPLOYEUR, AGE, SEXE, LIEN FROM APP.CLIENT  \n" +
"        UNION \n" +
"        SELECT NUM_AFFILIATION, PERCENTAGE, DATE_EXP, ASSURANCE, EMPLOYEUR, AGE, SEXE, LIEN FROM APP.CLIENT_RAMA \n" +
"    ) AS CLIENT ON APP.CREDIT.NUMERO_AFFILIE = CLIENT.NUM_AFFILIATION\n" +
"WHERE APP.INVOICE.HEURE > '2019-01-01 00:00:00' AND APP.INVOICE.HEURE < '2019-01-05 23:59:59' ";
        
        
        List<String> databases = Arrays.asList("ishyiga", "VINE2022-10-11");
        // Connect to the source databases
        Connection[] sourceCon = new Connection[databases.size()];
        Statement[] sourceStmt = new Statement[databases.size()];
        for (int i = 0; i < databases.size(); i++) {
            try {
                sourceCon[i] = DriverManager.getConnection("jdbc:derby://source-host:1527/" + databases.get(i) + ";create=true");
                sourceStmt[i] = sourceCon[i].createStatement();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        // Retrieve the data from the source databases
        ResultSet[] sourceRs = new ResultSet[databases.size()];
        for (int i = 0; i < databases.size(); i++) {
            try {
                sourceRs[i] = sourceStmt[i].executeQuery(sqlQuery);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        // Write the data to a CSV file
        File csvFile = new File("/Users/ntezi/Dev/PYTHON/SciData/data/extracted/consolidatedData.csv");
        try ( PrintWriter pw = new PrintWriter(csvFile)) {
            for (int i = 0; i < databases.size(); i++) {
                while (sourceRs[i].next()) {
                    int id = sourceRs[i].getInt("ID_INVOICE");
                    String code = sourceRs[i].getString("CODE");
                    pw.println(id + "," + code);
                }
            }
        } catch (SQLException | FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Close the connections
        try {
            for (int i = 0; i < databases.size(); i++) {
                sourceRs[i].close();
                sourceStmt[i].close();
                sourceCon[i].close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Create an S3 client
        AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();

        // Upload the CSV file to S3
        s3client.putObject(new PutObjectRequest("my-bucket", "consolidatedData.csv", csvFile));
    }
}
