package space.gavinklfong.demo.finance.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import space.gavinklfong.demo.finance.model.Transaction;
import space.gavinklfong.demo.finance.model.TransactionKey;
import space.gavinklfong.demo.finance.model.TransactionType;
import space.gavinklfong.demo.finance.schema.Account;
import space.gavinklfong.demo.finance.schema.AccountBalance;

import java.util.Map;
import java.util.function.Function;

@Slf4j
@Configuration
public class KafkaTopologyConfig {

    public static final String ACCOUNT_BALANCE_STATE_STORE = "account-balance-store";

    @Bean
    public Function<KStream<TransactionKey, Transaction>, KStream<Account, AccountBalance>> calculateAccountBalance() {
        return transactions -> transactions
                .process(AccountBalanceCalculator::new, Named.as("account-balance-calculator"), ACCOUNT_BALANCE_STATE_STORE)
                .flatMapValues(v -> v)
                .selectKey((key, value) -> buildKey(value.getAccount()))
                .peek((key, value) -> log.info("output - key: {}, value: {}", key, value), Named.as("log-output"));
    }

    private static Account buildKey(String account) {
        return Account.newBuilder()
                .setAccount(account)
                .build();
    }

    @Bean
    public StoreBuilder<KeyValueStore<String, String>> accountBalanceStateStore() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ACCOUNT_BALANCE_STATE_STORE),
                Serdes.String(),
                Serdes.String()
        );
    }

    @Bean
    public Serde<TransactionKey> transactionKeySerde() {
        return TransactionSerdes.transactionKey();
    }

    @Bean
    public Serde<Transaction> transactionSerde() {
        return TransactionSerdes.transaction();
    }

    @Bean
    public Serde<Account> accountKeySerde() {
        return TransactionSerdes.accountKey();
    }

    @Bean
    public Serde<AccountBalance> accountBalanceSerde() {
        return TransactionSerdes.accountBalance();
    }
}
