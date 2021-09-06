package com.davtor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.davtor.avro.Customer;
import com.davtor.config.KafkaStreamsConfig;
import com.davtor.config.KafkaTopologyConfig;
import com.davtor.config.TestSchemaRegistryConfig;
import com.davtor.config.properties.AppPropertiesConfig;
import com.davtor.json.Ticket;
import com.davtor.serde.SerdeFactory;

import lombok.extern.log4j.Log4j2;

@Log4j2
@DirtiesContext
@ExtendWith(SpringExtension.class)
@TestPropertySource({"classpath:application-test.properties"})
@EnableConfigurationProperties(AppPropertiesConfig.class)
@SpringJUnitConfig(classes = {TestSchemaRegistryConfig.class,
		SerdeFactory.class, KafkaStreamsConfig.class,
		KafkaTopologyConfig.class})
@EmbeddedKafka(partitions = 1, topics = {"${app.kafkaCustomerTopicName}",
		"${app.kafkaOutputTopicName}",
		"${app.kafkaTicketTopicName}"}, brokerPropertiesLocation = "classpath:application-test.properties")
public class AppHappyTest {

	@Autowired
	protected EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	@Qualifier("defaultKafkaAvroProducerFactory")
	protected DefaultKafkaProducerFactory<String, ?> kafkaAvroProducerFactory;

	@Autowired
	@Qualifier("defaultKafkaJsonProducerFactory")
	protected DefaultKafkaProducerFactory<String, ?> kafkaJsonProducerFactory;

	@Autowired
	@Qualifier("defaultKafkaJsonConsumerFactory")
	protected DefaultKafkaConsumerFactory<String, Ticket> kafkaConsumerFactory;

	@Autowired
	protected AppPropertiesConfig appProperties;

	protected Producer<String, Customer> customerProducer;

	protected Producer<String, Ticket> ticketProducer;

	protected Consumer<String, Ticket> ticketConsumer;

	@SuppressWarnings("unchecked")
	@BeforeEach
	public void init() {

		log.info("embeddedKafka brokers={}",
				embeddedKafkaBroker.getBrokersAsString());

		customerProducer = (Producer<String, Customer>) kafkaAvroProducerFactory
				.createProducer();
		ticketProducer = (Producer<String, Ticket>) kafkaJsonProducerFactory
				.createProducer();

		ticketConsumer = (Consumer<String, Ticket>) kafkaConsumerFactory
				.createConsumer();

		embeddedKafkaBroker.consumeFromAnEmbeddedTopic(ticketConsumer,
				appProperties.getKafkaOutputTopicName());
	}

	@AfterEach
	public void release() {
		customerProducer.close();
		ticketProducer.close();

		ticketConsumer.close();
	}

	@Test
	public void shouldProduceOneMessage()
			throws InterruptedException, ExecutionException {

		final String customerId = "S-117";
		final Customer customer = Customer.newBuilder()
				.setCustomerId(customerId).setName("David")
				.setLastName("Torres")
				.setBirthDate(LocalDate.of(1992, 12, 23).toString())
				.setHeight(172.0).setWeight(72.0).build();

		customerProducer.send(new ProducerRecord<String, Customer>(
				appProperties.getKafkaCustomerTopicName(), customerId,
				customer)).get();
		Thread.sleep(1000);
		final String expectedName = "Torres, David";
		final Ticket ticket = new Ticket();
		ticket.setFilled(true);
		ticket.setTicketId(1l);
		ticket.setCustomerId(customerId);
		ticket.setDate(LocalDate.of(1999, 12, 25).toString());

		ticketProducer.send(new ProducerRecord<String, Ticket>(
				appProperties.getKafkaTicketTopicName(), customerId, ticket))
				.get();

		final var outputRecord = KafkaTestUtils.getSingleRecord(ticketConsumer,
				appProperties.getKafkaOutputTopicName());

		assertThat(outputRecord).isNotNull();
		assertThat(outputRecord.value().getCustomerName())
				.isEqualTo(expectedName);

	}

}
