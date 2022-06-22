package com.kafka.tutorial.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.tutorial.libraryeventsproducer.controller.LibraryEventsController;
import com.kafka.tutorial.libraryeventsproducer.domain.Book;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEvent;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEventType;
import com.kafka.tutorial.libraryeventsproducer.producer.LibraryEventsProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;


//This annotation we can use if we're wrting unti TC
//for controller layers.So we have to tell it
//to spring for which controller we're writing the TC for.
@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc //Enables AutoConfig for MockMvc class
public class LibraryEventsControllerUnitTest {

    //Now this mockMvc will have access to
    // all the endpoints as part of LibraryController class.
    @Autowired
    private MockMvc mockMvc;

    //Dependencies are injected as mock beans.
    @MockBean
    private LibraryEventsProducer libraryEventsProducer;;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void postLibraryEventTest() throws Exception {
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

        String data = objectMapper.writeValueAsString(libraryEvent);

        //mocking service layer call
        doNothing().when(libraryEventsProducer).sendLibraryEvent_New(isA(LibraryEvent.class));

        mockMvc.perform( MockMvcRequestBuilders.post("/v1/libraryevent").
                content(data).contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isCreated());
    }

    @Test
    void postLibraryEventTest_4xx() throws Exception {
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .eventType(LibraryEventType.NEW)
                .book(book)
                .build();

        String data = objectMapper.writeValueAsString(libraryEvent);

        //mocking service layer call
        doNothing().when(libraryEventsProducer).sendLibraryEvent_New(isA(LibraryEvent.class));

        String expectedResult = "book.bookAuthor - must not be blank,book.bookId - must not be null";
        mockMvc.perform( MockMvcRequestBuilders.post("/v1/libraryevent").
                content(data).contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError())
                .andExpect(MockMvcResultMatchers.content().string(expectedResult));
    }
}
