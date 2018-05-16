import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class bankApp {

    // Logger initial
    private static final Logger log = LoggerFactory.getLogger(bankApp.class);

    // Kafka Environment on openShift + Local Environment
    private static final String BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS";
    private static final String TOPIC_TEMPERATURE = "TOPIC_TEMPERATURE";
    private static final String TOPIC_TEMPERATURE_MAX = "TOPIC_TEMPERATURE_MAX";

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC_TRANSACTIONS = "bank-transactions";
    private static final String DEFAULT_TOPIC_TRANSACTIONS_EXACTLY_ONCE = "bank-balance-exactly-once";

    // Decouple of the application component
    private KStreamBuilder builder;

    private Properties config;


    public bankApp(){
        this.builder = new KStreamBuilder();

        this.config = new Properties();

    }


    public Serde getJsonSerde(){
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        return jsonSerde;
    }


    public Properties getConfig(){
        return this.config;
    }



    public Properties defaultConfig(){

        String bootstrapServers = System.getenv().getOrDefault(BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS);
        this.config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.STATE_DIR_CONFIG, "my-state-store");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return config;
    }

    public KStream jsonNodeKStreamBuilder(String topicName){

        KStream<String, JsonNode> jsonNodeKStream =
                builder.stream(Serdes.String(), this.getJsonSerde(), topicName);

        return jsonNodeKStream;
    }

    public KStreamBuilder getBuilder(){
        return this.builder;
    }

    public static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        switch (transaction.get("type").asText()){
            case "add":
                newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
                break;
            case "delete":
                newBalance.put("balance", balance.get("balance").asInt() - transaction.get("amount").asInt());
            case "callBack":
                newBalance.put("balance", 0);
        }

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }

    public KafkaStreams createKafkaStrean(KStreamBuilder builder, Properties config){
        KafkaStreams streams = new KafkaStreams(builder, config);
        return streams;
    }

    public void startStream(KafkaStreams streams){
        streams.cleanUp();
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public void stopStream(KafkaStreams streams){
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.close();
    }

    public static void main(String[] args) {
        String topicTransaction = System.getenv().getOrDefault(TOPIC_TEMPERATURE, DEFAULT_TOPIC_TRANSACTIONS);
        String topicTemperatureMax = System.getenv().getOrDefault(TOPIC_TEMPERATURE_MAX, DEFAULT_TOPIC_TRANSACTIONS_EXACTLY_ONCE);

        bankApp rollBackApp = new bankApp();
        Properties config = rollBackApp.defaultConfig();
        Serde<JsonNode> jsonSerde = rollBackApp.getJsonSerde();

        KStream<String, JsonNode> bankTransactions =
                rollBackApp.jsonNodeKStreamBuilder(topicTransaction);

        //KStream<String, JsonNode> state =rollBackApp.jsonNodeKStreamBuilder("bank-transactions");

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Serdes.String(), jsonSerde)
                .aggregate(
                        () -> initialBalance,
                        ( key,  transaction,  balance) -> newBalance(transaction, balance),
                        jsonSerde,
                        "bank-balance-agg"
                );



        bankBalance.to(Serdes.String(), jsonSerde,topicTemperatureMax);



        KafkaStreams streams = rollBackApp.createKafkaStrean(rollBackApp.getBuilder(), config);

        rollBackApp.startStream(streams);

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
