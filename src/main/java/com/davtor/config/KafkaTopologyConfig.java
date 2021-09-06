package com.davtor.config;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.davtor.avro.Customer;
import com.davtor.config.properties.AppPropertiesConfig;
import com.davtor.json.Ticket;
import com.davtor.serde.SerdeFactory;

import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
@EnableKafkaStreams
public class KafkaTopologyConfig {

	@Autowired
	private StreamsBuilder streamsBuilder;

	@Autowired
	private SerdeFactory serdeFactory;

	@Autowired
	private AppPropertiesConfig appProperties;

	@Bean
	@SuppressWarnings("resource")
	public KStream<String, Ticket> kStream() {
		log.info("Creating stream....");

		final Serde<Customer> customerSerde = serdeFactory.createSpecificSerde();
		final JsonSerde<Ticket> ticketSerde;
		ticketSerde = new JsonSerde<>(Ticket.class).ignoreTypeHeaders().noTypeInfo();
		final var userTable = createUsersTable(customerSerde);

		final KStream<String, Ticket>  ticketStream = streamsBuilder.
				stream(appProperties.getKafkaTicketTopicName(), 
						Consumed.with(Serdes.String(), ticketSerde))
				.join(userTable, (ticket, user) -> {ticket.setCustomerName(createBuildName(user)); return ticket;});
		
		ticketStream.to(appProperties.getKafkaOutputTopicName(), 
				Produced.with(Serdes.String(), ticketSerde));

		log.info("Stream created successfully!");
		return ticketStream;
	}
	
	private String createBuildName(Customer user) {
		return user.getLastName() + ", " + user.getName();
	}

	private KTable<String, Customer> createUsersTable(Serde<Customer> customerSerde) {

		return streamsBuilder
				.stream(appProperties.getKafkaCustomerTopicName(), Consumed.with(Serdes.String(), customerSerde))
				.toTable();
	}

}
