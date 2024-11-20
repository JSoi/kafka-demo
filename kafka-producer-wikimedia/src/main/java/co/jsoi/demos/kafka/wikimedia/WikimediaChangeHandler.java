package co.jsoi.demos.kafka.wikimedia;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements EventHandler {
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        logger.info("opened");
    }

    @Override
    public void onClosed() {
        logger.info("closed");

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        logger.info("EVENT : {}", messageEvent.getData());

        // 비동기
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String s) {
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error in Stream Reading : {}", throwable);
    }
}
