package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am Consumer!");

        String consumerGroupId = "first_application";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", consumerGroupId);

        /**
         * 토픽을 어디서부터 읽을 건지에 대한 설정값
         * none : consumer group이 설정되어 있지 않으면 실패
         * earliest : topic의 처음 부터 읽는다(cli의 --from-beginning)
         * latest : 현재부터 읽는다
         */
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 현재 쓰레드 참조
        final Thread mainThread = Thread.currentThread();

        // 셧다운 훅 설정
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown Detected. Exit by Calling consumer.wakeup() ...");
            consumer.wakeup();
            // 메인 쓰레드가 코드를 실행할 수 있도록 조인시킴
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            //topic 구독
            consumer.subscribe(Arrays.asList(topic));
            // 데이터 폴링
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key : {}, Value : {}", record.key(), record.value());
                    log.info("Partition : {}, Offset : {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is Starting to ShutDown..");
        } catch (Exception e) {
            log.error("Unexcepted exception in the consumer", e);
        } finally {
            consumer.close();
            log.info("The consuer is now gracefully Shutdown");
        }
    }
}
