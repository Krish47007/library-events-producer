package com.kafka.tutorial.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.tutorial.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.net.Inet4Address;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Autowired
    private KafkaTemplate<Integer,String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    private static final String LIBRARY_EVENT_TOPIC = "library-event";

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        //If we use sendDefault then we dont have to specify the topic name
        //Here its going to read the default topic name from the application.yml file.
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {

                handleSuccess(key,value,result);
            }
        });
    }

    public void sendLibraryEvent_New(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key,value);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {

                handleSuccess(key,value,result);
            }
        });

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {

        return new ProducerRecord<>(LIBRARY_EVENT_TOPIC,null,key,value);
    }

    public void sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        try {
            //This is a blocking call - synchronous
            SendResult<Integer, String> sendResult = kafkaTemplate.sendDefault(key, value).get(3, TimeUnit.SECONDS);
            log.info("Send result is {}",sendResult.toString());
            handleSuccess(key,value,sendResult);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            handleFailure(e);
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result)
    {
        log.info("Message published for the key {} and value {} in partition {} and offset {}"
                ,key,value,result.getRecordMetadata().partition(),result.getRecordMetadata().offset());
    }

    private void handleFailure(Throwable ex)
    {
        log.error("Couldn't publish record due to {}",ex.getMessage());
    }

}
