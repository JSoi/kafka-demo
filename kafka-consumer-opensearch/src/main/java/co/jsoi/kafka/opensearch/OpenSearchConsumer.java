package co.jsoi.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static Logger log;

    public static void main(String[] args) throws IOException {
        KafkaConsumer<String, String> consumer;
        // create opensearch client
        log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
        // create kakfa client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        consumer = createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();

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
        try (openSearchClient) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikipedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikipedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been Created");
            } else {
                log.info("The Wikimedia Index already Exists");
            }
            consumer.subscribe(Collections.singleton("wikimedia.recentChange"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received {} Record(s)", recordCount);
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    // to Opensearch
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    try {
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
//                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                        log.info(indexResponse.getId());
                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        log.error("error sending opensearch request {}", e.getMessage());
                    }
                }

//                consumer.commitSync(); // manual commit
//                log.info("Offsets have been committed");
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} record(s)", bulkResponse.getItems().length);
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        log.error("BulkRequest Error {}", e.getMessage());
                    }
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer is Starting to ShutDown..");
        } catch (Exception e) {
            log.error("Unexcepted exception in the consumer", e);
        } finally {
            consumer.close();
            openSearchClient.close();
            log.info("The consumer is now gracefully Shutdown");
        }

    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        log.info("I am Consumer!");

        String consumerGroupId = "consumer-opensearch-demo";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:4092");

        // Consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", consumerGroupId);

        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("partition.assigment.strategy", CooperativeStickyAssignor.class.getName());
        properties.setProperty("partition.enable.auto.commit", "false");
//        properties.setProperty("group.instance.id", "value"); // for static assignments

        return new KafkaConsumer<>(properties);

    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }
}
