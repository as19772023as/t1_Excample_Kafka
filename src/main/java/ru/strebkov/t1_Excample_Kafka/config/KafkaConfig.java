package ru.strebkov.t1_Excample_Kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic topic() {
        return new NewTopic("topic1", 1, (short) 1); // имя, № топика, ?-реплик
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic("topic1.DLT", 1, (short) 1);
    }

    @Bean
    public RecordMessageConverter converter(){
        return new JsonMessageConverter();
    }

//    @Bean
//    public BatchMessagingMessageConverter batchMessagingMessageConverter(){
//        return new BatchMessagingMessageConverter(converter());
//    }
//    @Bean
//    public RecordMessageConverter converter() {
//        JsonMessageConverter converter = new JsonMessageConverter();
//        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
//        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
//        typeMapper.addTrustedPackages("com.example.kafkaexample.model");
//        Map<String, Class<?>> classMap = new HashMap<>();
//        classMap.put("foo", Foo2.class);
//        classMap.put("bar", Bar2.class);
//        typeMapper.setIdClassMapping(classMap);
//        converter.setTypeMapper(typeMapper);
//        return converter;
//    }


    //пользовательский обработчик ошибок, который будет обрабатывать исключение десериализации и
    // увеличивать смещение потребителя.
    // Это позволит нам пропустить недопустимое сообщение и продолжить использование.
    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaOperations) {

        // DeadLetterPublishingRecoverer просто публикует входящее содержимое ConsumerRecord.
        //Когда ErrorHandlingDeserializer2 обнаруживает исключение десериализации, в ConsumerRecord отсутствует поле value() (поскольку его невозможно десериализовать).
        //Вместо этого ошибка помещается в один из двух заголовков:

        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaOperations), new FixedBackOff(1000L, 2));

        // Header header = record.headers().lastHeader(headerName);
        //DeserializationException ex = (DeserializationException) new ObjectInputStream(
        //    new ByteArrayInputStream(header.value())).readObject();
        //with the original payload in ex.getData().

        //Простая реализация BackOff, обеспечивающая фиксированный интервал между двумя попытками и
        // максимальное количество повторных попыток.
    }
}
