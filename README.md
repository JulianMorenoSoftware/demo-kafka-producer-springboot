# demo-kafka productor

Aplicación Spring Boot que actúa como **productor de eventos** dentro de un ecosistema de demostración de Apache Kafka. Expone endpoints REST que, al ser invocados, publican eventos en distintos *topics* de Kafka para que otros servicios (en este caso, [`demo-kafka Consumidor`](https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot)) los consuman de forma asíncrona.

Este README está pensado para que cualquier persona del equipo, sin contexto previo, entienda **qué hace este servicio, cómo se comunica con el resto del ecosistema, y cómo levantarlo localmente**.

## Repositorios relacionados

Este proyecto es una pieza de un ecosistema de 3 repositorios que se complementan entre sí:

| Repositorio | Rol | Enlace |
|---|---|---|
| `kafka-broker-docker` | Infraestructura: levanta el broker de Kafka en Docker | https://github.com/JulianMorenoSoftware/kafka-broker-docker |
| **`demo-kafka producer`** (este repo) | Productor: expone APIs REST que publican eventos | https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot |
| `demo-kafka consumer` | Consumidor: escucha los topics y procesa los eventos | https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot |

## Arquitectura general

```
                     HTTP POST                         Kafka topic                    @KafkaListener
 Cliente/Postman/curl ────────► demo-kafka productor ────────────────► demo-kafka Consumidor
                                  (puerto 8080)                          (puerto 8081)
                                        │                                      │
                                        └──────────► Kafka broker ◄────────────┘
                                                    (192.168.2.10:9092)
```

1. Un cliente (Postman, curl, otro servicio) llama a un endpoint REST de **este** servicio.
2. El endpoint delega en un `Service` productor, que serializa el evento en JSON y lo publica en un topic de Kafka usando `KafkaTemplate`.
3. El broker de Kafka (ver [`kafka-broker-docker`](https://github.com/JulianMorenoSoftware/kafka-broker-docker)) persiste el mensaje en el topic correspondiente.
4. El servicio [`demo-kafka Consumidor`](https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot) tiene un `@KafkaListener` suscrito a ese topic, que recibe el evento y lo procesa (hoy solo hace `log`, pero es el punto de extensión para lógica de negocio real).

Este flujo es la esencia de una arquitectura *event-driven*: el productor no sabe (ni le importa) quién consume el evento, ni cuándo. Solo publica el hecho de que "algo ocurrió" en el topic, y cualquier número de consumidores puede reaccionar a eso de forma desacoplada.

## Stack técnico

- **Java 17**
- **Spring Boot 3.5.16** (`spring-boot-starter-parent`)
- **spring-boot-starter-web** — expone los endpoints REST
- **spring-kafka** — integración de Spring con Kafka (`KafkaTemplate`, configuración de productores)
- **kafka-streams** — dependencia declarada en el `pom.xml` para futuras topologías de streaming; **aún no se usa** en el código actual
- Tests: `spring-boot-starter-test`, `spring-kafka-test` (soporta `@EmbeddedKafka` para tests de integración sin broker real), `awaitility`

## Requisitos previos

Antes de ejecutar este servicio necesitas un broker de Kafka corriendo. Este proyecto **no** levanta Kafka por sí mismo — depende del repositorio de infraestructura:

👉 **[`kafka-broker-docker`](https://github.com/JulianMorenoSoftware/kafka-broker-docker)**

Pasos resumidos (ver el README de ese repo para el detalle completo):

```bash
git clone https://github.com/JulianMorenoSoftware/kafka-broker-docker.git
cd kafka-broker-docker
docker compose up -d
docker compose ps   # confirmar que "broker" y "kafka-ui" están "Up"
```

Esto levanta:
- El **broker de Kafka** (modo KRaft, sin Zookeeper), accesible en `localhost:9092` (o la IP del host, ver más abajo).
- **Kafka UI**, una interfaz web para inspeccionar topics y mensajes, en `http://localhost:9080`.

## Configuración (`application.yaml`)

```yaml
spring:
  application:
    name: demo-kafka
  kafka:
    bootstrap-servers: 192.168.2.10:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: demo-kafka-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.example.kafka.messaging

app:
  kafka:
    topics:
      greetings: greetings-topic
      sequences: count-topic
```

Explicación propiedad por propiedad:

- `spring.kafka.bootstrap-servers`: dirección `host:puerto` del broker de Kafka. **Importante:** está fijada a `192.168.2.10:9092`, la IP anunciada (`KAFKA_ADVERTISED_LISTENERS`) por el broker en `kafka-broker-docker`. Si tu máquina no tiene esa IP o el broker corre en otra red, deberás ajustar este valor (por ejemplo a `localhost:9092` si el broker corre en el mismo host).
- `spring.kafka.producer.key-serializer` / `value-serializer`: cómo se serializan la clave y el valor de cada mensaje antes de enviarlo. Clave como `String`, valor como JSON (usando `JsonSerializer` de Spring Kafka, que además añade un header con el nombre completo de la clase Java del evento).
- `spring.kafka.consumer.*`: este servicio también trae configuración de *consumer*, heredada de la plantilla común de Spring Initializr, aunque **este proyecto no tiene ningún `@KafkaListener`** (es puramente productor). Se deja por consistencia con el otro proyecto, pero no tiene efecto funcional aquí.
- `app.kafka.topics.greetings` / `app.kafka.topics.sequences`: nombres de los topics de Kafka, externalizados en vez de quedar como *magic strings* en el código. Se leen a través de la clase `KafkaTopicsProperties` (`@ConfigurationProperties(prefix = "app.kafka.topics")`), registrada automáticamente gracias a `@ConfigurationPropertiesScan` en `DemoKafkaApplication`.

**Puerto del servidor:** no se define `server.port` en este `application.yaml`, por lo que Spring Boot usa el puerto por defecto: **8080**.

## Modelo de eventos

| Evento (record Java) | Campos | Topic destino | Publicado por |
|---|---|---|---|
| `GreetingEvent` | `sender: String`, `message: String`, `createdAt: Instant` | `greetings-topic` | `GreetingProducer` |
| `SequenceEvent` | `value: long`, `createdAt: Instant` | `count-topic` | `SequenceProducer` |

```java
// messaging/GreetingEvent.java
public record GreetingEvent(String sender, String message, Instant createdAt) {
    public static GreetingEvent of(String sender, String message) {
        return new GreetingEvent(sender, message, Instant.now());
    }
}

// messaging/SequenceEvent.java
public record SequenceEvent(long value, Instant createdAt) {
    public static SequenceEvent of(long value) {
        return new SequenceEvent(value, Instant.now());
    }
}
```

`GreetingProducer` publica usando el `sender` como clave de partición (para que todos los saludos del mismo remitente vayan a la misma partición y se procesen en orden):

```java
public void send(GreetingEvent event) {
    kafkaTemplate.send(topicsProperties.greetings(), event.sender(), event);
}
```

`SequenceProducer` mantiene contadores en memoria (`ConcurrentHashMap<String, AtomicLong>`), uno por cada `name` de secuencia. Cada llamada incrementa el contador de esa secuencia, publica un `SequenceEvent` con el valor generado (usando `name` como clave de partición), y devuelve ese valor:

```java
public long next(String name) {
    long value = counters.computeIfAbsent(name, key -> new AtomicLong()).incrementAndGet();
    kafkaTemplate.send(topicsProperties.sequences(), name, SequenceEvent.of(value));
    return value;
}
```

⚠️ Al ser contadores **en memoria**, se reinician cada vez que se reinicia la aplicación — no persisten entre despliegues.

## Endpoints REST

### `POST /greetings/{sender}?message={message}`

Publica un `GreetingEvent` en `greetings-topic`.

```bash
curl -X POST "http://localhost:8080/greetings/juan?message=hola%20equipo"
```

Respuesta: `202 Accepted` (sin body) — el `202` indica que el evento fue *aceptado para publicación asíncrona*, no que ya fue procesado por el consumidor.

### `POST /sequences/{name}`

Genera el siguiente valor de la secuencia identificada por `name`, lo publica en `count-topic` como `SequenceEvent`, y devuelve el valor generado.

```bash
curl -X POST "http://localhost:8080/sequences/pedidos"
```

Respuesta: `202 Accepted` con el valor `Long` generado en el body (por ejemplo, `1`, luego `2`, etc. en llamadas sucesivas).

## Cómo se relaciona con el consumidor

El servicio [`demo-kafka Consumidor`](https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot) (puerto **8081**) está suscrito a los mismos topics que este productor publica:

- `greetings-topic` → consumido por `GreetingListener`, que loguea el saludo recibido.
- `count-topic` → consumido por `CountListener` (ver nota importante abajo).

Consulta el README del consumidor para el detalle de qué hace con cada evento: https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot#readme

## ⚠️ Nota conocida: contrato desalineado en `count-topic`

Al revisar ambos proyectos se detectó que **el topic `count-topic` tiene productor y consumidor con contratos de evento incompatibles**:

- Este productor publica `SequenceEvent(long value, Instant createdAt)` en `count-topic`.
- El consumidor (`CountListener`) espera deserializar `CountRequestEvent(int upperBound)` desde ese mismo topic.

Como `spring-kafka` usa `JsonSerializer`/`JsonDeserializer`, el productor incluye en cada mensaje un header con el nombre completo de la clase (`com.example.kafka.messaging.SequenceEvent`). El consumidor no tiene esa clase en su classpath (solo conoce `CountRequestEvent`), por lo que **si hoy se invoca `POST /sequences/{name}` con el consumidor corriendo, la deserialización del lado del consumidor fallará** (error de tipo/clase no encontrada), y ese mensaje terminará como registro fallido en el log del consumidor.

**Esto es intencional dejarlo documentado y no "silenciado":** es un ejemplo real de cómo un cambio de contrato de evento sin coordinación entre productor y consumidor rompe la integración, aunque el código compile perfecto en ambos lados por separado. Antes de usar este flujo en un demo en vivo, hay que alinear el esquema de evento (por ejemplo, unificando ambos proyectos para usar `CountRequestEvent` o `SequenceEvent`, no dos records distintos apuntando al mismo topic).

El flujo de **`greetings-topic` sí es consistente** de punta a punta: mismo record `GreetingEvent` en ambos proyectos.

## Cómo ejecutar todo el stack localmente

Orden recomendado de arranque:

1. **Levantar Kafka** (ver [`kafka-broker-docker`](https://github.com/JulianMorenoSoftware/kafka-broker-docker)):
   ```bash
   cd kafka-broker-docker
   docker compose up -d
   ```
2. **Levantar el consumidor** (para que esté escuchando antes de producir eventos):
   ```bash
   cd "demo-kafka Consumidor"
   ./mvnw spring-boot:run
   ```
3. **Levantar este productor**:
   ```bash
   cd "demo-kafka productor"
   ./mvnw spring-boot:run
   ```
4. **Probar el flujo de greetings** (extremo a extremo, funciona correctamente):
   ```bash
   curl -X POST "http://localhost:8080/greetings/juan?message=hola%20equipo"
   ```
   Deberías ver en los logs del consumidor una línea similar a:
   ```
   Saludo recibido de 'juan': hola equipo (2026-...)
   ```
5. (Opcional) Inspeccionar los mensajes publicados desde **Kafka UI**: http://localhost:9080

## Comandos Maven útiles

Usa siempre el wrapper (`./mvnw`), no una instalación de Maven del sistema, para garantizar la versión correcta:

```bash
./mvnw compile              # compilar
./mvnw test                 # ejecutar todos los tests
./mvnw test -Dtest=NombreDeLaClase            # ejecutar una clase de test específica
./mvnw test -Dtest=NombreDeLaClase#metodo     # ejecutar un método de test específico
./mvnw spring-boot:run       # ejecutar la aplicación localmente
./mvnw clean package         # generar el jar ejecutable (target/*.jar)
```

## Referencias

- Infraestructura de Kafka (Docker Compose): https://github.com/JulianMorenoSoftware/kafka-broker-docker
- Servicio productor (este repo): https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot
- Servicio consumidor: https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot
