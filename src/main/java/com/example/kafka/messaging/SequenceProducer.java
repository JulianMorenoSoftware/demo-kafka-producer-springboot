package com.example.kafka.messaging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Genera y publica valores consecutivos para secuencias identificadas por
 * nombre, exponiendo cada nuevo valor como un {@link SequenceEvent} en el
 * topic de secuencias configurado.
 *
 * <p>Mantiene un contador independiente en memoria por cada nombre de
 * secuencia, ya que este servicio es el único responsable de decidir el
 * siguiente valor de cada secuencia antes de publicarlo; no delega esa
 * responsabilidad a Kafka ni a un almacén externo. Se usa un {@link
 * ConcurrentHashMap} de {@link AtomicLong} porque múltiples peticiones HTTP
 * concurrentes pueden solicitar el siguiente valor de la misma secuencia, y
 * el incremento debe ser atómico para no repetir ni saltar valores.</p>
 */
@Service
public class SequenceProducer {

    private final KafkaTemplate<String, SequenceEvent> kafkaTemplate;
    private final KafkaTopicsProperties topicsProperties;
    private final ConcurrentHashMap<String, AtomicLong> countersByName = new ConcurrentHashMap<>();

    /**
     * Crea el productor inyectando sus dependencias por constructor.
     *
     * @param kafkaTemplate    plantilla de Kafka auto-configurada por Spring Boot
     * @param topicsProperties propiedades con los nombres de topic de la aplicación
     */
    public SequenceProducer(KafkaTemplate<String, SequenceEvent> kafkaTemplate,
                             KafkaTopicsProperties topicsProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicsProperties = topicsProperties;
    }

    /**
     * Genera el siguiente valor de la secuencia identificada por
     * {@code name} y lo publica como un {@link SequenceEvent} en el topic
     * de secuencias, usando el propio nombre como clave de partición para
     * que los eventos de una misma secuencia conserven orden relativo.
     *
     * @param name nombre de la secuencia cuyo siguiente valor se solicita
     * @return el valor generado para la secuencia, empezando en 1
     */
    public long next(String name) {
        long value = countersByName
                .computeIfAbsent(name, key -> new AtomicLong(0))
                .incrementAndGet();
        kafkaTemplate.send(topicsProperties.sequences(), name, SequenceEvent.of(value));
        return value;
    }
}
