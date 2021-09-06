package com.davtor.config;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.davtor.config.properties.AppPropertiesConfig;
import com.davtor.json.Ticket;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

@Configuration
public class TestSchemaRegistryConfig {

	@Autowired
	private AppPropertiesConfig appProperties;

	@Bean
	public SchemaRegistryClient schemaRegistryClient() {
		
		return new MockSchemaRegistryClient();
	}

	/** KafkaAvroSerializer that uses the MockSchemaRegistryClient */
	@Bean
	public KafkaAvroSerializer kafkaAvroSerializer(SchemaRegistryClient schemaRegistryClient) {
		
		final Map<String, Object> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, appProperties.getKafkaSchemaRegistryUrl(),
				KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

		return new KafkaAvroSerializer(schemaRegistryClient, config);
	}

	/** KafkaAvroDeserializer that uses the MockSchemaRegistryClient */
	@Bean
	public KafkaAvroDeserializer kafkaAvroDeserializer(SchemaRegistryClient schemaRegistryClient) {
		
		final Map<String, Object> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, appProperties.getKafkaSchemaRegistryUrl(),
				KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

		return new KafkaAvroDeserializer(schemaRegistryClient, config);
	}
	
	/**DefaultKafkaProducerFactory that uses MockSchemaRegistryClient through kafkaAvroSerializer instance */
	@Bean(name = "defaultKafkaAvroProducerFactory")
	public DefaultKafkaProducerFactory<?, ?> defaultKafkaAvroProducerFactory(KafkaAvroSerializer kafkaAvroSerializer){
	
		final Map<String, Object> config = Map.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,appProperties.getKafkaBootstrapServers());
		return  new DefaultKafkaProducerFactory<>(
				config, 
				new StringSerializer(), 
				kafkaAvroSerializer);
	}
	
	/**DefaultKafkaProducerFactory that produce JSON messages */
	@Bean(name = "defaultKafkaJsonProducerFactory")
	public DefaultKafkaProducerFactory<?, ?> defaultKafkaJsonProducerFactory(){
	
		final Map<String, Object> config = Map.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,appProperties.getKafkaBootstrapServers());
		
		return  new DefaultKafkaProducerFactory<>(
				config, 
				new StringSerializer(),
				new JsonSerializer<>());
	}
	
	/**DefaultKafkaProducerFactory that produce String messages */
	@Bean(name = "defaultKafkaStringProducerFactory")
	public DefaultKafkaProducerFactory<?, ?> defaultKafkaStringProducerFactory(){
	
		final Map<String, Object> config = Map.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,appProperties.getKafkaBootstrapServers());
		
		return  new DefaultKafkaProducerFactory<>(
				config, 
				new StringSerializer(),
				new StringSerializer());
	}
	
	/**DefaultKafkaConsumerFactory that uses MockSchemaRegistryClient through kafkaAvroDeserializer instance */
	@Bean(name = "defaultKafkaAvroConsumerFactory")
	public DefaultKafkaConsumerFactory<?, ?> defaultKafkaAvroConsumerFactory(KafkaAvroDeserializer kafkaAvroDeserializer){
		final Map<String,Object> config = KafkaTestUtils.consumerProps(appProperties.getKafkaBootstrapServers(), "testGroup", "true");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		return  new DefaultKafkaConsumerFactory<>(config, 
				new StringDeserializer(), 
				kafkaAvroDeserializer);
	} 
	
	/**DefaultKafkaConsumerFactory that uses MockSchemaRegistryClient through kafkaAvroDeserializer instance */
	@Bean(name = "defaultKafkaStringConsumerFactory")
	public DefaultKafkaConsumerFactory<?, ?> defaultKafkaStringConsumerFactory(){
		final Map<String,Object> config = KafkaTestUtils.consumerProps( appProperties.getKafkaBootstrapServers(), "testGroup", "true");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		return  new DefaultKafkaConsumerFactory<>(config, 
				new StringDeserializer(), 
				new StringDeserializer());
	} 
	
	/**DefaultKafkaConsumerFactory that uses MockSchemaRegistryClient through kafkaAvroDeserializer instance */
	@SuppressWarnings("resource")
	@Bean(name = "defaultKafkaJsonConsumerFactory")
	public DefaultKafkaConsumerFactory<?, ?> defaultKafkaJsonConsumerFactory(){
		final Map<String,Object> config = KafkaTestUtils.consumerProps( appProperties.getKafkaBootstrapServers(), "testGroup", "true");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		return  new DefaultKafkaConsumerFactory<>(config, 
				new StringDeserializer(), 
				new JsonDeserializer<>(Ticket.class).ignoreTypeHeaders());
	} 
	

}
