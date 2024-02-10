package space.gavinklfong.demo.finance.topology;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import space.gavinklfong.demo.finance.model.Transaction;
import space.gavinklfong.demo.finance.model.TransactionKey;
import space.gavinklfong.demo.finance.model.TransactionType;
import space.gavinklfong.demo.finance.schema.Account;
import space.gavinklfong.demo.finance.schema.AccountBalance;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class TopologyTest {

    private static final String TEST_CLASS = TopologyTest.class.getName();
    private static final String MOCK_BOOTSTRAP_URL = String.format("%s-bootstrap:1234", TEST_CLASS);
    private static final String MOCK_SCHEMA_REGISTRY_URL = String.format("mock://%s-schema-registry:1234", TEST_CLASS);
    private static final Map<String, String> JSON_SERDE_PROPS = Map.of(JsonDeserializer.TRUSTED_PACKAGES, "space.gavinklfong.demo.finance.model");

    private final static String INPUT_TOPIC = "transactions";
    private final static String OUTPUT_TOPIC = "account-balances";
    private TestInputTopic<TransactionKey, Transaction> inputTopic;
    private TestOutputTopic<Account, AccountBalance> outputTopic;
    private TopologyTestDriver testDriver;

    @BeforeEach
    void setup() throws RestClientException, IOException {

        // setup Input JSON Serdes
        JsonSerde<TransactionKey> transactionKeySerde = new JsonSerde<>(TransactionKey.class);
        transactionKeySerde.configure(JSON_SERDE_PROPS, true);
        JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class);
        transactionSerde.configure(JSON_SERDE_PROPS, false);

        // setup output AVRO Serdes
        SerdeFactory avroSerdeFactory = new SerdeFactory(MOCK_SCHEMA_REGISTRY_URL, getMockSchemaRegistryClient());
        Serde<Account> accountKeySerde = avroSerdeFactory.getSerde(true);
        Serde<AccountBalance> accountBalanceSerde = avroSerdeFactory.getSerde(false);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // create input stream
        KStream<TransactionKey, Transaction> transactionKStream = streamsBuilder.stream(INPUT_TOPIC,
                Consumed.with(transactionKeySerde, transactionSerde));

        // register account balance calculation function
        KafkaTopologyConfig kafkaTopologyConfig = new KafkaTopologyConfig();
        streamsBuilder.addStateStore(kafkaTopologyConfig.accountBalanceStateStore());
        kafkaTopologyConfig.calculateAccountBalance()
                .apply(transactionKStream)
                .to(OUTPUT_TOPIC, Produced.with(accountKeySerde, accountBalanceSerde));

        Topology topology = streamsBuilder.build();

        // create input topic and output topic
        testDriver = new TopologyTestDriver(topology, getStreamsConfig());
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC,
                transactionKeySerde.serializer(), transactionSerde.serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC,
                accountKeySerde.deserializer(), accountBalanceSerde.deserializer());
    }

    private MockSchemaRegistryClient getMockSchemaRegistryClient() throws RestClientException, IOException {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
        mockSchemaRegistryClient.register("account-balances-key", new AvroSchema(Account.SCHEMA$));
        mockSchemaRegistryClient.register("account-balances-value", new AvroSchema(AccountBalance.SCHEMA$));
        return mockSchemaRegistryClient;
    }

    @AfterEach
    void cleanUp() {
        testDriver.close();
    }

    private Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "account-balance-calculator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, MOCK_BOOTSTRAP_URL);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        return props;
    }

    @Test
    void givenTransferTransaction_testTransactionFilter_thenOutputTransferTransaction() {
        Instant transactionTimestamp = Instant.now().truncatedTo(ChronoUnit.HOURS);

        Account fromAccount = Account.newBuilder()
                .setAccount("001-0001-101")
                .build();

        Account toAccount = Account.newBuilder()
                .setAccount("001-0001-102")
                .build();

        TransactionKey transactionKey = TransactionKey.builder()
                .id(UUID.randomUUID())
                .fromAccount(fromAccount.getAccount())
                .toAccount(toAccount.getAccount())
                .build();

        Transaction transfer = Transaction.builder()
                .id(transactionKey.getId())
                .type(TransactionType.TRANSFER)
                .toAccount(transactionKey.getToAccount())
                .fromAccount(transactionKey.getFromAccount())
                .amount(new BigDecimal("10.00"))
                .timestamp(LocalDateTime.ofInstant(transactionTimestamp, ZoneOffset.UTC))
                .build();

        AccountBalance fromAccountBalance = AccountBalance.newBuilder()
                .setAccount(fromAccount.getAccount())
                .setAmount(new BigDecimal("-10.00"))
                .setTimestamp(transactionTimestamp)
                .build();

        AccountBalance toAccountBalance = AccountBalance.newBuilder()
                .setAccount(toAccount.getAccount())
                .setAmount(new BigDecimal("10.00"))
                .setTimestamp(transactionTimestamp)
                .build();

        inputTopic.pipeInput(transactionKey, transfer);
        List<KeyValue<Account, AccountBalance>> outputs = outputTopic.readKeyValuesToList();
        assertThat(outputs)
                .hasSize(2)
                .containsExactlyInAnyOrder(
                        KeyValue.pair(fromAccount, fromAccountBalance),
                        KeyValue.pair(toAccount, toAccountBalance)
                );
    }

}
