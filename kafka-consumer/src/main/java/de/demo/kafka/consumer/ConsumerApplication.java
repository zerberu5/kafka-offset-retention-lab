package de.demo.kafka.consumer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

@SpringBootApplication
public class ConsumerApplication {

    private static final Logger log = LoggerFactory.getLogger(ConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    @KafkaListener(topics = "${app.topic.name}")
    public void listen(ConsumerRecord<String, String> record) {
        log.info("Received partition={} offset={} payload='{}'",
                record.partition(), record.offset(), record.value());
    }

    /**
     * Wird von Spring Boot automatisch an die Listener-Container-Factory gehängt.
     * Hinweis: Bei auto.offset.reset=latest committet Spring Kafka die ermittelte Position bereits
     * VOR diesem Callback (assignmentCommitOption=LATEST_ONLY_NO_TX), "committed" ist dann nicht
     * leer. Ob ein Reset stattfand, zeigen zuverlässig die Zeilen
     * "Found no committed offset" (ConsumerCoordinator) und "Resetting offset" (SubscriptionState).
     */
    @Bean
    ConsumerAwareRebalanceListener rebalanceLogger(
            @Value("${spring.kafka.consumer.auto-offset-reset}") String autoOffsetReset) {
        return new ConsumerAwareRebalanceListener() {

            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("Partitions revoked: {}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions));
                Map<TopicPartition, Long> begin = consumer.beginningOffsets(partitions);
                Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
                for (TopicPartition tp : partitions) {
                    OffsetAndMetadata c = committed.get(tp);
                    long position = consumer.position(tp);
                    String where = position == end.get(tp) ? "= log end"
                            : position == begin.get(tp) ? "= log begin"
                            : "mid-log, lag=" + (end.get(tp) - position);
                    log.info("Partition assigned: {} | log=[{}..{}) | committed={} | start position={} {} | auto.offset.reset={}",
                            tp, begin.get(tp), end.get(tp), c == null ? "none" : c.offset(),
                            position, where, autoOffsetReset);
                }
            }
        };
    }
}
