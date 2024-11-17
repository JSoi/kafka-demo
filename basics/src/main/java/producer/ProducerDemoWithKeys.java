package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World!");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // batch size 조정 - sticky partitioner에 묶음으로 메시지 전달
        properties.setProperty("batch.size", "400");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int oc = 0; oc < 10; oc++) {
            for (int i = 0; i < 5; i++) {
                // topic : first_topic
                // value : first_value 레코드 발행
                // key : key+i 형태의 레코드 발행
                /**
                 * key를 갖는 메시지는 해당하는 Partitioner로 분배됨
                 */
                String topic = "first_topic";
                String key = "key" + i;
                String value = "new_value" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                // 수신, 혹은 예외 발생 시 실행
                producer.send(record, (md, e) -> {
                    if (e == null) {
                        log.info(
                                "Received New Metadata {} {} : {} Partition : {}",
                                md.topic(),
                                key,
                                value,
                                md.partition()
                        );
                    } else {
                        log.error("Error While Producing : {}", e);
                    }
                });
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("Error Catched {}", e);
                }
            }
        }

        // 모든 메시지 produce 전까지 block
        producer.flush();

        // Close Producer
        producer.close();
    }
}
