package ai.ntezi.etl.extract;
import org.apache.avro.Schema;
import org.apache.trevni.avro.RandomData;

import java.util.Iterator;

public class Avro {
    public static void main(String [] args){
        Schema schema = new Schema.Parser().parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.acme\",\n" +
                "     \"name\": \"Test\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"name\", \"type\": \"string\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\" },\n" +
                "       { \"name\": \"sex\", \"type\": \"string\" },\n" +
                "       { \"name\": \"active\", \"type\": \"boolean\" }\n" +
                "     ]\n" +
                "}");

        Iterator<Object> it = new RandomData(schema, 1).iterator();
        System.out.println(it.next());
    }
}
