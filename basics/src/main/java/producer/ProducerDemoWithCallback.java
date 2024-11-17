package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World!");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // topic : first_topic, value : first_value 레코드 발행
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "new_value");
        // 수신, 혹은 예외 발생 시 실행
        producer.send(record, (md, e) -> {
            if (e == null) {
                log.info(
                        "Received New Metadata\n" +
                                "Topic : {}\n" +
                                "Partition : {}\n" +
                                "Offset : {}\n" +
                                "TimeStamp : {}"
                        ,
                        md.topic(),
                        md.partition(),
                        md.offset(),
                        md.timestamp()
                );
            } else {
                log.error("Error While Producing : {}", e);
            }
        });

        // 모든 메시지 produce 전까지 block
        producer.flush();

        // Close Producer
        producer.close();
    }
}
