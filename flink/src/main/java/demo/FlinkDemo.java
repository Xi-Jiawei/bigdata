package demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "192.168.61.130:9092");
        properties.put("bootstrap.servers", "47.98.176.164:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("mykafka", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);
        stream.print();

        env.execute();
    }
}
