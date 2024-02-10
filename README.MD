# Demonstration of Spring Cloud Stream Binder - Kafka Streams

## Overview
This is to demonstrate the implementation of Kafka Streams with Spring Cloud Stream Binder. 

The transaction-producer generates transactions every 1 second to a Kafka topic `transactions` and a topology
calculate account balance in a real time based on the previous balance.

The following modules give you the idea how to build stateless and stateful data stream processing:
- **transaction-filter-topology**: Stateless topology of transaction filter that output only transfer transaction
- **account-balance-calculation-topology**: Stateful topology of account balance calculation using Kafka key-value state store

## How to start local environment

Run this command to start the local environment in docker containers:
```
cd docker
docker compose up
```

Kafka will be started. You can access the Kafka using [Web UI](http://localhost:3030)

Next, run the transaction producer
```
docker run --rm --net=host whalebig27/transaction-producer:latest
```
Then, you will see the messages published into topic `transactions`

**Run Transaction Filter Topology**
Execute the main class `TransactionFilterApplication` in `transaction-filter-topology` module 
Once the topology is up and running, check the outputs on topic `transfer-transactions`

**Run Account Balance Calculation Topology**
Execute the main class `AccountBalanceCalculationTopologyApplication` in `account-balance-calculation-topology` module
Once the topology is up and running, check the outputs on topic `account-balances`

