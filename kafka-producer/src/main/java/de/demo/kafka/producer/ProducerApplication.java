package de.demo.kafka.producer;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class ProducerApplication {

    private static final Logger log = LoggerFactory.getLogger(ProducerApplication.class);

    private final AtomicLong counter = new AtomicLong();
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public ProducerApplication(KafkaTemplate<String, String> kafkaTemplate,
                               @Value("${app.topic.name}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Bean
    NewTopic demoTopic(@Value("${app.topic.partitions}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    @PostMapping("/send")
    public List<String> send(@RequestParam(defaultValue = "1") int count) {
        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String payload = "msg-%d @ %s".formatted(counter.incrementAndGet(),
                    LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));
            futures.add(kafkaTemplate.send(topic, payload));
        }
        return futures.stream().map(CompletableFuture::join).map(result -> {
            RecordMetadata meta = result.getRecordMetadata();
            String line = "%s -> partition=%d offset=%d".formatted(
                    result.getProducerRecord().value(), meta.partition(), meta.offset());
            log.info("Sent {}", line);
            return line;
        }).toList();
    }
}
