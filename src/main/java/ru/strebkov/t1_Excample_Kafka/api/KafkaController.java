package ru.strebkov.t1_Excample_Kafka.api;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.strebkov.t1_Excample_Kafka.model.Bar1;
import ru.strebkov.t1_Excample_Kafka.model.Foo1;

@RestController
@RequiredArgsConstructor
@Slf4j
public class KafkaController {

    private final KafkaTemplate<Object, Object> template;
    // класс из библСпринг для Кафки
    // Он предоставляет высокую абстракцию для отправки сообщений в темы Kafka с помощью простой и
    // декларативной модели программирования. Обычно используется в приложениях Spring Boot.
    // KafkaTemplate инкапсулирует конфигурацию производителя Kafka, включая сериализацию и
    // разбиение сообщений. Он предоставляет методы для отправки сообщений в синхронном или асинхронном режиме.

    @PostMapping(path = "/send/foo/{what}")
    public void sendFoo(@PathVariable String what) {
        log.info("sendFoo: {}", what);
        template.send("foos", new Foo1(what));
    }
    @PostMapping(path = "/send/bar/{what}")
    public void sendBar(@PathVariable String what) {
        log.info("sendBar: {}", what);
        template.send("bars", new Bar1(what));
    }

    @PostMapping(path = "/send/unknown/{what}")
    public void sendUnknown(@PathVariable String what) {
        log.info("sendUnknown: {}", what);
        template.send("bars", what);
    }

//    @PostMapping(path = "/send/transaction/foo/{what}")
//    public void sendTransactionFoo(@PathVariable String what) {
//        log.info("sendTransactionFoo: {}", what);
//        template.executeInTransaction(kafkaTemplates -> {
//            StringUtils.commaDelimitedListToSet(what).stream()
//                    .map(Foo1::new)
//                    .forEach(foo1 -> kafkaTemplates.send("topic1", foo1));
//            return null;
//        });
//    }
//
//    @PostMapping(path = "/send/{what}")
//    public void sendWhat(@PathVariable String what) {
//        log.info("sendWhat: {}", what);
//        template.send("topic3", what);
//    }
}

