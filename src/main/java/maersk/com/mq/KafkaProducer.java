package maersk.com.mq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/*
 * Kafka Producer
 */
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

	@Value("${application.name:kafka-producer}")
	private String clientId;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@Bean
	public ProducerFactory<Object, Object> producerFactory() {
		
		Map<String, Object> properties = new HashMap<>();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.destBootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, this.destLinger);
		properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, this.destTransactionTimeout);
		properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, this.destBlockMS);
		properties.put(ProducerConfig.ACKS_CONFIG, this.destAcks);
		
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, this.clientId);
        try {
			properties.put(ProducerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());

        } catch (UnknownHostException e) {
			// do nothing ....
		}
		
		if (this._debug) {
			log.info("******* eyecatcher *******");
			log.info("Starting producer");
		}
				
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
