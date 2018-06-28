package hyron.kafka.demo.KafkaMysqlDemo;

import hyron.kafka.demo.KafkaMysqlDemo.bean.User;
import hyron.kafka.demo.KafkaMysqlDemo.processor.FnameCountProcessor;
import hyron.kafka.demo.SpringBootDemo.domain.MyObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class FnameCountProcessorDemo {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-fname-count-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-node0:9092,kafka-node1:9092,kafka-node2:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<MyObject> userJsonDeserializer = new JsonDeserializer<>(MyObject.class);
        StringDeserializer stringDeserializer = new StringDeserializer();

        JsonDeserializer<User> countJsonDeserializer= new JsonDeserializer<>(User.class);
        JsonSerializer<User> countJsonSerializer = new JsonSerializer<>();
        Serde<User> countSerde = Serdes.serdeFrom(countJsonSerializer, countJsonDeserializer);
        Serde<String> keySerde = Serdes.String();

        Topology builder = new Topology();

        builder.addSource("Source", stringDeserializer, userJsonDeserializer, "user-topic");

        builder.addProcessor("Process", new FnameCountProcessor(), "Source");
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("user-fname-count"),
                keySerde,
                countSerde),
                "Process");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("user-fname-count-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}