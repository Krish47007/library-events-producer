package com.kafka.tutorial.libraryeventsproducer.controller;

import com.kafka.tutorial.libraryeventsproducer.domain.Book;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEvent;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEventType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)  // if not given it runs the TC in default port 8080.
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Test
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

        assertEquals(HttpStatus.CREATED,response.getStatusCode());
    }
}
