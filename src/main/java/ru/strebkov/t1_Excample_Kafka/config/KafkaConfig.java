package ru.strebkov.t1_Excample_Kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.util.backoff.FixedBackOff;
import ru.strebkov.t1_Excample_Kafka.model.Bar2;
import ru.strebkov.t1_Excample_Kafka.model.Foo2;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic topic() {
        return new NewTopic("topic1", 1, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic("topic2", 1, (short) 1);
    }

    @Bean
    public NewTopic topic3() {
        return new NewTopic("topic3", 1, (short) 1);
    }
    @Bean
    public NewTopic dlt() {
        return new NewTopic("topic1.DLT", 1, (short) 1);
    }



    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaOperations), new FixedBackOff(1000L, 2));
        // DeadLetterPublishingRecoverer просто публикует входящее содержимое ConsumerRecord.
        //Когда ErrorHandlingDeserializer2 обнаруживает исключение десериализации, в ConsumerRecord отсутствует поле value() (поскольку его невозможно десериализовать).
        //Вместо этого ошибка помещается в один из двух заголовков:

        // Header header = record.headers().lastHeader(headerName);
        //DeserializationException ex = (DeserializationException) new ObjectInputStream(
        //    new ByteArrayInputStream(header.value())).readObject();
        //with the original payload in ex.getData().

        //Простая реализация BackOff, обеспечивающая фиксированный интервал между двумя попытками и
        // максимальное количество повторных попыток.
    }


    @Bean
    public NewTopic foos() {
        return new NewTopic("foos", 1, (short) 1);
    }

    @Bean
    public NewTopic bars() {
        return new NewTopic("bars", 1, (short) 1);
    }
}
