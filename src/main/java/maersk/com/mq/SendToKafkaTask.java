package maersk.com.mq;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.tomcat.jni.Thread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.headers.MQDataException;

import maersk.com.mq.listener.MQConnection;

/*
 * Task to send messages to Kafka
 */
public class SendToKafkaTask implements Callable<Integer> {

	private Logger log = Logger.getLogger(this.getClass());

	private KafkaTemplate<String, String> kafkaTemplate;
	public void setKafkaTemplate(KafkaTemplate<?,?> val) {
		this.kafkaTemplate = (KafkaTemplate<String, String>) val;
	}
	
	private String topicName;
	public void setTopicName(String val) {
		this.topicName = val;
	}

	private MQConnection conn;
	public void setMQConnection(MQConnection conn) {
		this.conn = conn;
	}

	private Map<String, String> rfh2;
	public void setRFH2Headers(Map rfh2) {
		this.rfh2 = rfh2;
	}

	private String payLoad;
	
	public SendToKafkaTask(String payLoad) {
		this.payLoad = payLoad;
	}
	
	private Integer ret = 0;
	
	@Override
	public Integer call() throws Exception {

		/*
		 * if we have a key on the rfh2, then use it, otherwise dont
		 */
		String key = null;
		ListenableFuture<SendResult<String,String>> future = null;
		if (this.rfh2.containsKey("key")) {
			key = this.rfh2.get("key");
			future = kafkaTemplate.send(this.topicName, key, this.payLoad);
		} else {
			future = kafkaTemplate.send(this.topicName, this.payLoad);
		}
		
		/*
		 * Send 
		 */
		future.addCallback(new ListenableFutureCallback<SendResult<String,String>>() {
	
			@Override
			public void onSuccess(SendResult<String,String> sendResult) {		
				
				try {
					successfullSend();
					
				} catch (MQException e) {
					try {
						rollBack();

					} catch (MQException e1) {
						log.error("Unable to commit transaction to MQ after successfully sending messages to Kafka");
					}
				}
				ret = 0;

			}

			@Override
			public void onFailure(Throwable ex) {

				try {
					processFailures(ex);

				} catch (MQException e) {
					log.error("Message was unable to be rolled back to MQ " + e.getReason() );
					log.error("Message : " + e.getMessage() );
				}
				ret = 2;					

			}
		});
		
		return ret;
	}

	protected synchronized void successfullSend() throws MQException {
		this.conn.commit();
	}

	protected synchronized void processFailures(Throwable ex) throws MQException {
		rollBack();
	}
	
	protected synchronized void rollBack() throws MQException {
		this.conn.rollBack();
	}

	

}
