package app.com.mq;

/*
 * Run the 'MQ to Kafka' API
 */
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@ComponentScan("app.com.mq.KafkaProducer")
@ComponentScan("app.com.mq.listener.MQConnection")
@ComponentScan("app.com.mq.listener.MQConsumerListener")
@SpringBootApplication
@EnableAsync
public class AoMqToKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(AoMqToKafkaApplication.class, args);
	}
	

}
