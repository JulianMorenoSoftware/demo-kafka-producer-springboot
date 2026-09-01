package com.example.kafka.messaging;

import java.time.Instant;

/**
 * Evento inmutable que representa el siguiente valor generado para una
 * secuencia publicada a través de Kafka.
 *
 * <p>Se modela como {@code record} porque es un objeto de valor: una vez
 * creado no cambia, y su igualdad se basa exclusivamente en sus datos. Se
 * serializa a JSON en el productor mediante los serializadores de Spring
 * Kafka configurados en {@code application.yaml}.</p>
 *
 * @param value     valor generado para la secuencia
 * @param createdAt instante en el que se generó el evento
 */
public record SequenceEvent(long value, Instant createdAt) {

    /**
     * Crea un {@link SequenceEvent} asignando automáticamente el instante
     * de creación al momento actual.
     *
     * @param value valor generado para la secuencia
     * @return un nuevo evento con {@code createdAt} igual a {@link Instant#now()}
     */
    public static SequenceEvent of(long value) {
        return new SequenceEvent(value, Instant.now());
    }
}
