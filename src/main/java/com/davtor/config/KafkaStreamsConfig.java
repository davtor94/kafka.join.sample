package com.davtor.config;

import java.util.HashMap;
import java.util.Map;



import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import com.davtor.config.properties.AppPropertiesConfig;

import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class KafkaStreamsConfig {

	@Autowired
	private ConfigurableApplicationContext appContext;

	private final AppPropertiesConfig appProperties;

	public KafkaStreamsConfig(AppPropertiesConfig appProperties) {
		this.appProperties = appProperties;
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kStreamsConfig() {
		log.info("Creating stream properties....");
		final String bootstrapServers = appProperties.getKafkaBootstrapServers();
		final Map<String, Object> props = new HashMap<>(appProperties.getKafkaExtraOptions());
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, appProperties.getKafkaReplicationFactor());
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appProperties.getKafkaApplicationId());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
		props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
				RecoveringDeserializationExceptionHandler.class);
		props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER,
				recoverer(bootstrapServers));

		addSaslAuthProperties(props);
		final KafkaStreamsConfiguration kStreamConfig = new KafkaStreamsConfiguration(props);
		log.info("Stream properties created successfully");
		return kStreamConfig;
	}

	private void addSaslAuthProperties(final Map<String, Object> props) {
		final String username = appProperties.getKafkaAuthUsername();
		final String password = appProperties.getKafkaAuthPassword();
		final boolean validCredentials = username != null && !username.isEmpty() && password != null
				&& !password.isEmpty();

		if (validCredentials) {
			log.info("Adding SASL authentication properties...");
			props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
			props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
			props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
					"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
					username, password));
		}

	}

	private DeadLetterPublishingRecoverer recoverer(String bootstrapServers) {

		final Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		addSaslAuthProperties(props);
		final ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(props);
		final KafkaOperations<String, String> kafkaOperations = new KafkaTemplate<>(producerFactory);

		return new DeadLetterPublishingRecoverer(kafkaOperations, (record, ex) -> {

			log.error("Couldn't deserialize message with key=[{}] and value=[{}]!", toPrintable(record.key()),
					toPrintable(record.value()));

			if (ex.getCause() != null && ex.getCause().getClass().getPackageName().equals("java.net")) {
				log.fatal("Schema registry client is not being able to connect with the server, killing app.", ex);
				SpringApplication.exit(appContext, () -> 999);
			}

			return new TopicPartition(appProperties.getKafkaInputDlqTopicName(), -1);
		});
	}

	/** Best-effort attempt to transform the given object into a printable object */
	private Object toPrintable(Object object) {

		try {
			if (object instanceof byte[]) {
				return new String((byte[]) object);
			}
		} catch (Exception ex) {

		}

		return object;
	}
	
	@Bean
	public StreamsBuilderFactoryBeanCustomizer customizer(ApplicationContext context) {
		
		return fb -> {
			fb.setStateListener((newState, oldState) -> {
				log.info("State transition from {}  to {}", oldState, newState);
				if(newState == State.ERROR) {
					log.fatal("The stream is dead , Shutting down the application!");
					SpringApplication.exit(context, () -> 666);
				}
				
			});
			fb.setUncaughtExceptionHandler( ( thread, ex) -> log.error( String.format("The following error occurred on the thread= [%s]!", thread.getName()),ex));
		};
		
	}
	
	

}
