/**
 * Created by Oleksii Kolisnyk on 14.01.17.
 * Free for uncommercial use.
 * @author Oliksii Kolisnyk
 * @version 1.0
 */
    import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
    import com.fasterxml.jackson.databind.ObjectMapper;
    import org.apache.kafka.clients.consumer.ConsumerRecords;
    import org.apache.kafka.clients.consumer.ConsumerRecord;
    import org.apache.kafka.clients.consumer.KafkaConsumer;
    import org.apache.spark.SparkConf;
    import org.apache.spark.api.java.JavaRDD;
    import org.apache.spark.api.java.JavaSparkContext;
    import java.io.IOException;
    import java.util.*;

/**
 * Create Kafka consumer for receiving JSON message,
 * creating Spark's job for saving data to Cassandra database.
 */
public class MessageKafkaConsumer{

    /**
     * Configuration for a Spark application.
     */
    private static SparkConf sparkConf = new SparkConf()
            .setAppName("Calculation")
            .set("spark.cassandra.connection.host", "127.0.0.1");

    /**
     * A Java-friendly version of SparkContext
     * that returns JavaRDDs and works with Java collections instead of Scala ones.
     */
    private static JavaSparkContext sc = new JavaSparkContext(sparkConf);

    /**
     * Set parameters for Cassandra database
     */
    private static final String cassandraKeySpaceName = "testkeyspace";
    private static final String cassandraKeySpaceTableName = "testkeyspace";

    /**
     * Set parameters for Kafka consumer
     */
    private static String topicName = "test";
    private static String gotMessage;
    private static Properties properties = new Properties();

    /**
     * Marker for stopping listener of Kafka
     */
    public static boolean run = true;


    /**
     * Create constructor with initialization Kafka's properties
     */
    MessageKafkaConsumer(){
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
    /**
     * Main method for application
     *
     * @value consumer creates new Kafka consumer
     * @value gotMessage is JSON received from Kafka
     * with properties have been set in constructor
     */
    public static void main(String[] args) throws Exception {
       new MessageKafkaConsumer();
       KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
       consumer.subscribe(Arrays.asList(topicName));
       while (run) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                gotMessage = record.value();
                convertingJSONToSpark(gotMessage);
            }
        }
    }

    /**
     * Main method for parsing JSON to JavaRDD and saving result from Spark to Cassandrs
     *
     * @value mapper converts JSON to Java Object
     * @value countries is list of entries with countries, received in JSON
     * @value ctry is list of countries, received in JSON
     * @value tempRDD is Spark storage of countries
     * @throws IOException if not String message received
     */
    private static void convertingJSONToSpark (String message){
        ObjectMapper mapper = new ObjectMapper();
        try {
            List<Map<String, Integer>> countries = mapper.readValue(message, List.class);
            List<Country> ctry = new ArrayList<>();
            countries.forEach((countryMap) -> {
                for (Map.Entry<String, Integer> country : countryMap.entrySet()) {
                    ctry.add(new Country(country.getKey(), country.getValue()));
                }
            });
            JavaRDD<Country> tempRDD = sc.parallelize(ctry);

            javaFunctions(tempRDD).writerBuilder(cassandraKeySpaceName, cassandraKeySpaceTableName, mapToRow(Country.class)).saveToCassandra();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
