package ru.strebkov.t1_Excample_Kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.strebkov.t1_Excample_Kafka.model.Foo2;

import java.util.List;

@Service
@Slf4j
//@KafkaListener(id = "multiGroup", topics = {"foos", "bars"})
@RequiredArgsConstructor
public class KafkaExampleListener {

    private final KafkaTemplate<Object, Object> template;
    // для отправки сообщений в темы Kafka,
    // включая сериализацию и разбиение сообщений


    @KafkaListener(id = "fooGroup", topics = "topic1")
    public void listen(Foo2 foo2) {
        log.info("Received: {}", foo2);
        if (foo2.getFoo().startsWith("fail")) {
            throw new RuntimeException();
        }
        log.info("OK");
    }


    @KafkaListener(id = "dltGroup", topics = "topic1.DLT")
    public void listen1(byte[] in) {
        log.info("Received listen1: {}", new String(in));
    }

}