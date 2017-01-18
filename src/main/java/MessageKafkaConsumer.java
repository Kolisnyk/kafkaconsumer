/**
 * Created by oleksii on 14.01.17.
 */

    import java.util.*;

    import org.apache.kafka.clients.consumer.ConsumerRecords;
    import org.apache.kafka.clients.consumer.ConsumerRecord;
    import org.apache.kafka.clients.consumer.KafkaConsumer;
    import org.apache.spark.SparkConf;
    import org.apache.spark.api.java.JavaRDD;
    import org.apache.spark.api.java.JavaSparkContext;
    import com.fasterxml.jackson.databind.ObjectMapper;
    import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
    import com.datastax.driver.core.Session;
    import com.datastax.spark.connector.cql.CassandraConnector;
    import java.io.IOException;


public class MessageKafkaConsumer{
    public static SparkConf sparkConf = new SparkConf()
            .setAppName("Calculation")
            .setMaster("local[*]")
            .set("spark.cassandra.connection.host", "127.0.0.1");
    public static JavaSparkContext sc = new JavaSparkContext(sparkConf);

    public static String gotMessage;
    public static Properties properties = new Properties();
    public static String topicName = "test";


    MessageKafkaConsumer(){
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static void main(String[] args) throws Exception {
        new MessageKafkaConsumer();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
       consumer.subscribe(Arrays.asList(topicName));
       while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                gotMessage = record.value();
                System.out.println(gotMessage);
                convertingJSONToSpark(gotMessage);
            }
        }
    }

    public static void convertingJSONToSpark (String message){
        ObjectMapper mapper = new ObjectMapper();
        try {
            List<Map<String, Integer>> countries = mapper.readValue(message, List.class);
            List<Country> ctry = new ArrayList<Country>();
            countries.forEach((countryMap) -> {
                for (Map.Entry<String, Integer> country : countryMap.entrySet()) {
                    ctry.add(new Country(country.getKey(), country.getValue()));
                }
            });
            JavaRDD<Country> tempRDD = sc.parallelize(ctry);
            javaFunctions(tempRDD).writerBuilder("testkeyspace", "country", mapToRow(Country.class)).saveToCassandra();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
