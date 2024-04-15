package ru.strebkov.t1_Excample_Kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import ru.strebkov.t1_Excample_Kafka.model.Bar2;
import ru.strebkov.t1_Excample_Kafka.model.Foo1;
import ru.strebkov.t1_Excample_Kafka.model.Foo2;

import java.util.List;

@Service
@Slf4j
//@KafkaListener(id = "multiGroup", topics = {"foos", "bars"})
@RequiredArgsConstructor
public class KafkaExampleListener {

    //  пример транзакции, либо все сообщения или нет
    private final KafkaTemplate<Object, Object> template;

    // для отправки сообщений в темы Kafka,
    // включая сериализацию и разбиение сообщений



    //@RetryableTopic повторной попытке неудачной обработки записей.
    // @RetryableTopic с другой стороны, это неблокирующая повторная попытка,
    // поскольку при каждом сбое неудачная запись перемещается в другую тему.
    // Это позволяет обрабатывать последующие записи во время повторных попыток.
    // Полезно, если строгий порядок не важен.
    //  attempts = "3" N - попыток 3 по дефолту,
    @RetryableTopic(attempts = "5", backoff = @Backoff(delay = 2000, maxDelay = 10000, multiplier = 2))
    @KafkaListener(id = "fooGroup", topics = "topic3")
    public void listen3(String in,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received from listen3: {}, topic: {}, offset: {}", in, topic, offset);
        if (in.startsWith("fail")) {
            throw new RuntimeException("failed");
        }
    }

    //@DltHandler- Аннотация для определения метода, который должен обрабатывать сообщение темы DLT.
    // У метода могут быть те же параметры, что и у KafkaListener метода (сообщение, подтверждение и т.д.).
    //Аннотированный метод должен находиться в том же классе, что и соответствующая KafkaListener аннотация.
    // тема мертвых писем (DLT)

    @DltHandler
    public void listenDlt(String in,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Received from DLT: {}, topic: {}, offset: {}", in, topic, offset);
    }
    // В реальных проектах обычно повторяют попытку обработки события в случае ошибок перед отправкой его в DLT.
    // Этого можно легко достичь,
    // используя механизм неблокирующих попыток, предоставляемый Spring Kafka.

    // В этой статье мы изучили три различные стратегии DLT. Первая - это стратегия FAIL_ON_ERROR,
    // когда пользователь DLT не будет пытаться повторно обработать событие в случае сбоя. Напротив,
    // стратегия ALWAYS_RETRY_ON_ERROR гарантирует, что пользователь DLT попытается повторно обработать событие
    // в случае сбоя. Это значение используется по умолчанию, когда явно не задана никакая другая стратегия.
    // Последней является стратегия NO_DLT, которая полностью отключает механизм DLT.
}