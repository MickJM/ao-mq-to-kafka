package maersk.com.mq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@ComponentScan("maersk.com.mq.KafkaProducer")
@ComponentScan("maersk.com.mq.listener.MQConnection")
@ComponentScan("maersk.com.mq.listener.MQConsumerListener")
@SpringBootApplication
@EnableAsync
public class AoMqToKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(AoMqToKafkaApplication.class, args);
	}
	

}
