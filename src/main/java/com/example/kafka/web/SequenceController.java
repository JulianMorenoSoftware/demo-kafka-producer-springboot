package com.example.kafka.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.messaging.SequenceProducer;

/**
 * Expone un endpoint HTTP para solicitar el siguiente valor de una
 * secuencia y disparar su publicación en Kafka, a modo de ejemplo
 * demostrativo del flujo productor.
 */
@RestController
public class SequenceController {

    private final SequenceProducer sequenceProducer;

    /**
     * Crea el controlador inyectando el productor de secuencias por
     * constructor.
     *
     * @param sequenceProducer servicio encargado de generar y publicar el siguiente valor en Kafka
     */
    public SequenceController(SequenceProducer sequenceProducer) {
        this.sequenceProducer = sequenceProducer;
    }

    /**
     * Solicita el siguiente valor de la secuencia indicada y lo publica en
     * Kafka.
     *
     * @param name nombre de la secuencia cuyo siguiente valor se solicita
     * @return {@code 202 Accepted} con el valor generado una vez que el evento se ha enviado al productor
     */
    @PostMapping("/sequences/{name}")
    public ResponseEntity<Long> nextValue(@PathVariable String name) {
        long value = sequenceProducer.next(name);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(value);
    }
}
