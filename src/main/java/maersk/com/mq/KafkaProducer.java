package maersk.com.mq;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class KafkaProducer {

	private Logger log = Logger.getLogger(this.getClass());
	
	@Value("${application.debug:true}")
	private boolean _debug;
	
	// Get all the parameters
	@Value("${kafka.dest.bootstrap.servers:}")
	private String destBootstrapServers;
	@Value("${kafka.dest.username:}")
	private String destUsername;
	@Value("${kafka.dest.password:}")
	private String destPassword;
	@Value("${kafka.dest.login.module:org.apache.kafka.common.security.plain.PlainLoginModule}")
	private String destLoginModule;
	@Value("${kafka.dest.sasl.mechanism:PLAIN}")
	private String destSaslMechanism;
	@Value("${kafka.dest.sasl.protocol:SASL_SSL}")
	private String destSaslProtocol;
	@Value("${kafka.dest.truststore.location:}")
	private String destTruststoreLocation;
	@Value("${kafka.dest.truststore.password:}")
	private String destTruststorePassword;
	@Value("${kafka.dest.linger:1}")
	private int destLinger;
	@Value("${kafka.dest.transaction.timeout:5000}")
	private int destTransactionTimeout;
	@Value("${kafka.dest.block:5000}")
	private int destBlockMS;
	@Value("${kafka.dest.acks:1}")
	private String destAcks;

	@Value("${spring.application.name:kafka-producer}")
	private String clientId;

	
	@Bean
	public ProducerFactory<Object, Object> producerFactory() {
		
		//LoadRunTimeParameters();
		
		Map<String, Object> properties = new HashMap<>();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destBootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, destLinger);

		properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, destTransactionTimeout);
		properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, destBlockMS);
		properties.put(ProducerConfig.ACKS_CONFIG,destAcks);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG,this.clientId);
		
		//properties.put("client.id", this.clientId);
		//properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-producer");
		//properties.put("transaction.timeout.ms", 5000);
		//properties.put("max.block.ms", 5000);
		//properties.put("acks", "1");
		if (this._debug) {
			log.info("******* eyecatcherS");
			log.info("Starting producer");
		}
		
		/*
		 * Testing using embedded
		 */
		//properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
		//		"org.apache.kafka.clients.producer.internals.DefaultPartitioner");
				
		
		addSaslProperties(properties, destSaslMechanism, destLoginModule, destUsername, destPassword, destSaslProtocol);
		addTruststoreProperties(properties, destTruststoreLocation, destTruststorePassword);

		return new DefaultKafkaProducerFactory<>(properties);
	}
	
	private void addSaslProperties(Map<String, Object> properties, String mechanism, String loginModule, String username, 
			String password, String protocol) {
		if (!StringUtils.isEmpty(username)) {
			properties.put("security.protocol", protocol);
			properties.put("sasl.mechanism", mechanism);
			properties.put("sasl.jaas.config",
					loginModule + " required username=" + username + " password=" + password + ";");
		}
	}

	private void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
		if (!StringUtils.isEmpty(location)) {
			properties.put("ssl.truststore.location", location);
			properties.put("ssl.truststore.password", password);
		}
		properties.put("ssl.keymanager.algorithm", "SunX509");

	}

	
}
