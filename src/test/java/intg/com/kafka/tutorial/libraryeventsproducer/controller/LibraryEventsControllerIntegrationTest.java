package com.kafka.tutorial.libraryeventsproducer.controller;

import com.kafka.tutorial.libraryeventsproducer.domain.Book;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEvent;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)  // if not given it runs the TC in default port 8080.
@EmbeddedKafka(topics = {"library-events"},partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers = ${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers = ${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    private TestRestTemplate testRestTemplate;

    //This gets created when we start our Test class
    //With @EmbeddedKafka
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    //Kafka Consumer instance
    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String,Object> config = new HashMap<>(KafkaTestUtils.consumerProps("group1","true",embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(config,new IntegerDeserializer(),new StringDeserializer())
                                                 .createConsumer();
        //Consume from all topics
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        //This needs to be shut down otherwise we might have problems
        // if we have multiple TC. Its a good practice also.
        consumer.close();
    }

    @Test
    @Timeout(value = 5,unit = TimeUnit.SECONDS)
    void postLibraryEventTest() {

        Book book = Book.builder()
                    .bookId(456)
                    .bookAuthor("Krish")
                    .bookName("Kafka Using Spring Boot")
                    .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .eventType(LibraryEventType.NEW)
                .book(book)
                .build();

        final String URL = "/v1/libraryevent";

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> httpEntity = new HttpEntity<>(libraryEvent,headers);
        ResponseEntity<LibraryEvent> response = testRestTemplate.exchange(URL, HttpMethod.POST, httpEntity, LibraryEvent.class);

        //Verifying the HttpStatus codes.
        assertEquals(HttpStatus.CREATED,response.getStatusCode());

        //Verify the record published.
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");

        String expectedValue = "{\"libraryEventId\":null,\"eventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Krish\"}}";
        String actualValue = consumerRecord.value();

        assertEquals(expectedValue,actualValue);

    }
}
