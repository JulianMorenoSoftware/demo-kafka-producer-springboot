package com.example.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

/**
 * Verifica de extremo a extremo que {@link SequenceProducer} publica en el
 * topic de secuencias un {@link SequenceEvent} por cada llamada a {@link
 * SequenceProducer#next(String)}, con contadores independientes por nombre
 * de secuencia, usando el broker embebido de {@code spring-kafka-test} en
 * lugar de un Kafka real.
 *
 * <p>Este proyecto es únicamente productor: no existe ningún {@code
 * @KafkaListener} para {@link SequenceEvent}. Por ello, la verificación se
 * hace con un {@link Consumer} de Kafka creado y controlado directamente
 * desde el test, en vez de registrar un consumidor de producción o auxiliar
 * basado en anotaciones.</p>
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "${app.kafka.topics.sequences}")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class SequenceKafkaIntegrationTest {

    @Autowired
    private SequenceProducer sequenceProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${app.kafka.topics.sequences}")
    private String sequencesTopic;

    private Consumer<String, SequenceEvent> consumer;

    @BeforeEach
    void createConsumer() {
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("sequence-test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka.messaging");

        DefaultKafkaConsumerFactory<String, SequenceEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, sequencesTopic);
    }

    @AfterEach
    void closeConsumer() {
        consumer.close();
    }

    @Test
    void nextValueIsPublishedAndIncrementsPerSequenceName() {
        long firstValueForOrders = sequenceProducer.next("orders");
        assertThat(firstValueForOrders).isEqualTo(1L);
        assertThat(nextReceivedEvent().value()).isEqualTo(1L);

        long secondValueForOrders = sequenceProducer.next("orders");
        assertThat(secondValueForOrders).isEqualTo(2L);
        assertThat(nextReceivedEvent().value()).isEqualTo(2L);

        long firstValueForInvoices = sequenceProducer.next("invoices");
        assertThat(firstValueForInvoices).isEqualTo(1L);
        assertThat(nextReceivedEvent().value()).isEqualTo(1L);
    }

    /**
     * Espera y devuelve el siguiente {@link SequenceEvent} disponible en el
     * topic de secuencias, sondeando el broker embebido hasta que llegue o
     * se agote el tiempo de espera.
     *
     * @return el siguiente evento recibido del topic bajo prueba
     */
    private SequenceEvent nextReceivedEvent() {
        return await().atMost(10, TimeUnit.SECONDS).until(() -> {
            ConsumerRecords<String, SequenceEvent> records = consumer.poll(Duration.ofMillis(200));
            if (records.isEmpty()) {
                return null;
            }
            ConsumerRecord<String, SequenceEvent> record = records.iterator().next();
            return record.value();
        }, event -> event != null);
    }
}
