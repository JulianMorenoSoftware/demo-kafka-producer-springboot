package com.example.kafka.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.messaging.GreetingEvent;
import com.example.kafka.messaging.GreetingProducer;

/**
 * Expone un endpoint HTTP para disparar el envío de un {@link GreetingEvent}
 * a Kafka, a modo de ejemplo demostrativo del flujo productor/consumidor.
 */
@RestController
public class GreetingController {

    private final GreetingProducer greetingProducer;

    /**
     * Crea el controlador inyectando el productor de saludos por constructor.
     *
     * @param greetingProducer servicio encargado de publicar el evento en Kafka
     */
    public GreetingController(GreetingProducer greetingProducer) {
        this.greetingProducer = greetingProducer;
    }

    /**
     * Publica un saludo en Kafka con el remitente y mensaje indicados.
     *
     * @param sender  nombre de quien envía el saludo
     * @param message contenido del saludo
     * @return {@code 202 Accepted} una vez que el evento se ha enviado al productor
     */
    @PostMapping("/greetings/{sender}")
    public ResponseEntity<Void> sendGreeting(@PathVariable String sender, @RequestParam String message) {
        greetingProducer.send(GreetingEvent.of(sender, message));
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
