package ai.ntezi.etl.extract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.ResultSet;
import java.sql.SQLException;


public class Transform {
    public void transformInvoices(ResultSet resultSet) throws SQLException, JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        // create a new JSON object
        ObjectNode objectNode = objectMapper.createObjectNode();
        /*while (resultSet.next()) {
            *//*String affiliateNumber = resultSet.getString("NUM_AFFILIATION");
            String primaryBeneficiary = resultSet.getString("BENEFICIAIRE");
            String firstName = resultSet.getString("PRENOM_CLIENT");
            String lastName = resultSet.getString("NOM_CLIENT");
            String code = resultSet.getString("CODE");
            String expirationDate = resultSet.getString("DATE_EXP");*//*
            // add properties to the object
        }*/

        objectNode.put("affiliateNumber", resultSet.getString("NUM_AFFILIATION"));
        objectNode.put("primaryBeneficiary", resultSet.getString("BENEFICIAIRE"));
        objectNode.put("firstName", resultSet.getString("PRENOM_CLIENT"));
        objectNode.put("lastName", resultSet.getString("NOM_CLIENT"));
        objectNode.put("code", resultSet.getString("CODE"));
        objectNode.put("expirationDate", resultSet.getString("DATE_EXP"));
        // Convert the result set into a JSON object
        // convert the JSON object to a string
        String jsonString = objectMapper.writeValueAsString(objectNode);
        System.out.println(jsonString);


    }
}
