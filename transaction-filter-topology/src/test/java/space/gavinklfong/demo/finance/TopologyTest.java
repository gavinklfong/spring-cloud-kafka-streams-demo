package space.gavinklfong.demo.finance;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import space.gavinklfong.demo.finance.model.Transaction;
import space.gavinklfong.demo.finance.model.TransactionKey;
import space.gavinklfong.demo.finance.model.TransactionType;
import space.gavinklfong.demo.finance.topology.KafkaTopologyConfig;
import space.gavinklfong.demo.finance.topology.TransactionSerdes;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class TopologyTest {
    private final static String INPUT_TOPIC = "transactions";
    private final static String OUTPUT_TOPIC = "filtered-transactions";
    private TestInputTopic<TransactionKey, Transaction> inputTopic;
    private TestOutputTopic<TransactionKey, Transaction> outputTopic;

    @BeforeEach
    void setup() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // create input stream
        KStream<TransactionKey, Transaction> transactionKStream = streamsBuilder.stream(INPUT_TOPIC);

        // register filter function
        KafkaTopologyConfig kafkaTopologyConfig = new KafkaTopologyConfig();
        kafkaTopologyConfig.filterForTransferTransaction().apply(transactionKStream).to(OUTPUT_TOPIC);

        Topology topology = streamsBuilder.build();

        // create input topic and output topic
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, getStreamsConfig());
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, TransactionSerdes.transactionKey().serializer(), TransactionSerdes.transaction().serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, TransactionSerdes.transactionKey().deserializer(), TransactionSerdes.transaction().deserializer());
    }

    private Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-filter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "space.gavinklfong.demo.finance.model");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, TransactionSerdes.transactionKey().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TransactionSerdes.transaction().getClass().getName());
        return props;
    }

    @Test
    void givenTransferTransaction_testTransactionFilter_thenOutputTransferTransaction() {
        TransactionKey transactionKey = TransactionKey.builder()
                .id(UUID.randomUUID())
                .toAccount("001-0001-101")
                .fromAccount("001-0001-102")
                .build();

        Transaction transfer = Transaction.builder()
                .id(transactionKey.getId())
                .type(TransactionType.TRANSFER)
                .toAccount(transactionKey.getToAccount())
                .fromAccount(transactionKey.getFromAccount())
                .amount(BigDecimal.TEN)
                .build();

        inputTopic.pipeInput(transactionKey, transfer);
        assertThat(outputTopic.readKeyValuesToList())
                .hasSize(1)
                .containsExactly(KeyValue.pair(transactionKey, transfer));
    }

    @ParameterizedTest
    @EnumSource(mode= EnumSource.Mode.EXCLUDE, names = { "TRANSFER" })
    void givenOtherTransaction_testTransactionFilter_thenOutputNothing(TransactionType type) {
        TransactionKey transactionKey = TransactionKey.builder()
                .id(UUID.randomUUID())
                .toAccount("001-0001-101")
                .fromAccount("001-0001-102")
                .build();

        Transaction transfer = Transaction.builder()
                .id(transactionKey.getId())
                .type(type)
                .toAccount(transactionKey.getToAccount())
                .fromAccount(transactionKey.getFromAccount())
                .amount(BigDecimal.TEN)
                .build();

        inputTopic.pipeInput(transactionKey, transfer);
        assertThat(outputTopic.readKeyValuesToList()).isEmpty();
    }


}
