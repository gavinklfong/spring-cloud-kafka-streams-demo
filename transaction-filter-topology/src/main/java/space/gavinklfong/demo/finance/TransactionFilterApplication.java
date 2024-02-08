package space.gavinklfong.demo.finance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class TransactionFilterApplication {
    public static void main(String[] args) {
        SpringApplication.run(TransactionFilterApplication.class, args);
    }
}

