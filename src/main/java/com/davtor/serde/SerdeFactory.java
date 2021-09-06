package com.davtor.serde;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;

import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.davtor.config.properties.AppPropertiesConfig;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@Component
public class SerdeFactory {

	@Autowired(required = false)
	private SchemaRegistryClient schemaRegistryClient;

	private final Map<String, Object> serdeConfig;

	public SerdeFactory(AppPropertiesConfig appProperties) {

		this.serdeConfig = Map.of(SCHEMA_REGISTRY_URL_CONFIG, appProperties.getKafkaSchemaRegistryUrl(),
				BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO", CLIENT_NAMESPACE + USER_INFO_CONFIG,
				appProperties.getKafkaSchemaRegistrySecret(), SPECIFIC_AVRO_READER_CONFIG, true

		);
	}

	/** Creates a new SpecificAvroSerde with the default properties */
	public <T extends SpecificRecord> Serde<T> createSpecificSerde() {
		
		final Serde<T> serde = (schemaRegistryClient != null) ? new SpecificAvroSerde<>(schemaRegistryClient)
				: new SpecificAvroSerde<>();
		
		serde.configure(serdeConfig, false);
		return serde;
	}

}
