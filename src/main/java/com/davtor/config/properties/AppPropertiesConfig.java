package com.davtor.config.properties;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Configuration
@ConfigurationProperties(prefix = "app")
public class AppPropertiesConfig implements InitializingBean {

	private String kafkaBootstrapServers;
	private String kafkaAuthUsername;
	private String kafkaAuthPassword;
	private String kafkaApplicationId;
	private int kafkaReplicationFactor = 3;
	private String kafkaPrefixTopicName;
	private String kafkaInputDlqTopicName;

	private String kafkaSchemaRegistryUrl;
	private String kafkaSchemaRegistrySecret;

	private String kafkaCustomerTopicName;
	private String kafkaTicketTopicName;

	private String kafkaOutputTopicName;

	private Map<String, String> kafkaExtraOptions = Collections.emptyMap();

	@Override
	public void afterPropertiesSet() throws Exception {

		Objects.requireNonNull(kafkaBootstrapServers, "bootstrapServers cannot be null!");
		Objects.requireNonNull(kafkaApplicationId, "applicationId cannot be null!");
		Objects.requireNonNull(kafkaPrefixTopicName, "prefixTopicName cannot be null!");

		Objects.requireNonNull(kafkaCustomerTopicName, "kafkaCustomerTopicName cannot be null");
		Objects.requireNonNull(kafkaTicketTopicName, "kafkaTicketTopicName cannot be null");
		Objects.requireNonNull(kafkaOutputTopicName, "kafkaOutputTopicName cannot be null");

		kafkaInputDlqTopicName = buildTopicName(kafkaInputDlqTopicName, ".dlq");

	}

	private String buildTopicName(String topicName, String topicSuffix) {
		return topicName != null ? topicName : kafkaPrefixTopicName + topicSuffix;
	}

}
