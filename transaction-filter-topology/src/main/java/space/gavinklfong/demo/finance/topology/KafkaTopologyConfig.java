package space.gavinklfong.demo.finance.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import space.gavinklfong.demo.finance.model.Transaction;
import space.gavinklfong.demo.finance.model.TransactionKey;
import space.gavinklfong.demo.finance.model.TransactionType;

import java.util.function.Function;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaTopologyConfig {

    @Bean
    public Function<KStream<TransactionKey, Transaction>, KStream<TransactionKey, Transaction>> filterForTransferTransaction() {
        return input -> input
                .peek((key, value) -> log.info("input - key: {}, value: {}", key, value), Named.as("log-input"))
                .filter((key, value) -> value.getType().equals(TransactionType.TRANSFER), Named.as("transaction-filter"))
                .peek((key, value) -> log.info("output - key: {}, value: {}", key, value), Named.as("log-output"));
    }

}
