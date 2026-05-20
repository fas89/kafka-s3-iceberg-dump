package com.example.iceberg.producer;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.example.iceberg.common.Env;
import com.example.iceberg.common.KafkaTls;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Mutual-TLS Kafka producer that generates synthetic JSON events for the demo.
 *
 * <p>Replaces the kcat {@code scripts/datagen.sh}. Two modes:
 * <ul>
 *   <li><b>continuous</b> (default) — emits one event every {@code INTERVAL_MS}
 *       forever, as the streaming data source;</li>
 *   <li><b>bounded</b> ({@code --count N}) — emits exactly {@code N} events at
 *       full throughput and exits, for benchmark runs.</li>
 * </ul>
 *
 * <p>Each event matches the schema every engine ingests:
 * {@code {id, event_type, user_id, amount, event_time}}.
 */
public final class EventProducer {

  private static final String[] EVENT_TYPES =
      {"click", "view", "purchase", "scroll", "signup"};

  private EventProducer() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> opt = parseArgs(args);
    final String bootstrap =
        opt.getOrDefault("bootstrap", Env.get("KAFKA_BOOTSTRAP", "kafka:9092"));
    final String topic = opt.getOrDefault("topic", Env.get("KAFKA_TOPIC", "events"));
    final long count = Long.parseLong(opt.getOrDefault("count", Env.get("RECORD_COUNT", "0")));
    final long intervalMs =
        Long.parseLong(opt.getOrDefault("interval-ms", Env.get("INTERVAL_MS", "500")));
    final boolean continuous = count <= 0;

    ObjectMapper mapper = new ObjectMapper();
    Random random = new Random();
    AtomicLong delivered = new AtomicLong();
    AtomicLong failed = new AtomicLong();
    AtomicBoolean running = new AtomicBoolean(true);
    CountDownLatch stopped = new CountDownLatch(1);

    // Continuous mode runs until the container is stopped: the shutdown hook
    // flips `running` and blocks until the loop has flushed and signalled.
    if (continuous) {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    running.set(false);
                    try {
                      stopped.await(15, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                      Thread.currentThread().interrupt();
                    }
                  },
                  "producer-shutdown"));
    }

    System.out.println(
        "event-producer -> bootstrap=" + bootstrap + " topic=" + topic
            + (continuous
                ? " mode=continuous interval=" + intervalMs + "ms"
                : " mode=bounded count=" + count));

    Producer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap));
    long start = System.currentTimeMillis();
    long produced = 0;
    try {
      while (continuous ? running.get() : produced < count) {
        String userId = "user-" + (random.nextInt(1000) + 1);
        String value = mapper.writeValueAsString(buildEvent(userId, random));
        producer.send(
            new ProducerRecord<>(topic, userId, value),
            (metadata, exception) -> {
              if (exception == null) {
                delivered.incrementAndGet();
              } else {
                failed.incrementAndGet();
              }
            });
        produced++;
        if (continuous && intervalMs > 0) {
          Thread.sleep(intervalMs);
        }
      }
    } finally {
      producer.flush();
      producer.close();
      printMetrics(topic, produced, delivered.get(), failed.get(), start);
      stopped.countDown();
    }
  }

  /** Builds one synthetic event as an ordered map ready for JSON serialization. */
  private static Map<String, Object> buildEvent(String userId, Random random) {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("id", UUID.randomUUID().toString());
    event.put("event_type", EVENT_TYPES[random.nextInt(EVENT_TYPES.length)]);
    event.put("user_id", userId);
    event.put("amount", Math.round(random.nextDouble() * 100.0 * 100.0) / 100.0);
    event.put("event_time", Instant.now().toString());
    return event;
  }

  private static Properties producerConfig(String bootstrap) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // Idempotent producer prevents duplicates on transient retries.
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    // Throughput tuning for bounded benchmark runs.
    p.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    p.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
    p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    // mTLS to the Kafka broker (ssl.client.auth=required).
    p.putAll(KafkaTls.clientProperties("producer"));
    return p;
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> opt = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--") && i + 1 < args.length) {
        opt.put(args[i].substring(2), args[++i]);
      }
    }
    return opt;
  }

  /** Prints a one-line JSON metrics record the benchmark harness can parse. */
  private static void printMetrics(
      String topic, long requested, long delivered, long failed, long startMs) {
    double elapsed = Math.max((System.currentTimeMillis() - startMs) / 1000.0, 1e-6);
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("stage", "produce");
    m.put("topic", topic);
    m.put("requested", requested);
    m.put("delivered", delivered);
    m.put("failed", failed);
    m.put("elapsed_s", Math.round(elapsed * 100.0) / 100.0);
    m.put("throughput_rps", Math.round(requested / elapsed * 10.0) / 10.0);
    try {
      System.out.println(new ObjectMapper().writeValueAsString(m));
    } catch (Exception e) {
      System.out.println(m); // fallback — still human-readable
    }
  }
}
