package com.kafka.tutorial.libraryeventsproducer.controller;

import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEvent;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEventType;
import com.kafka.tutorial.libraryeventsproducer.producer.LibraryEventsProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    private LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody LibraryEvent libraryEvent)
    {
        try
        {
            libraryEvent.setEventType(LibraryEventType.NEW);

            //invoke Kafka Producer
            log.info("Before sending ");
            //libraryEventsProducer.sendLibraryEvent(libraryEvent);
            //libraryEventsProducer.sendLibraryEventSynchronous(libraryEvent);
            libraryEventsProducer.sendLibraryEvent_New(libraryEvent);
            log.info("After sending ");
            return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
        }catch (Exception ex)
        {
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).build();
        }
    }
}
