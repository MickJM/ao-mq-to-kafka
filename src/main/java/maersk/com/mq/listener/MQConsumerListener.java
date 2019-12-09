package maersk.com.mq.listener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.mq.MQException;

import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQRFH2;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import maersk.com.kafka.constants.MQKafkaConstants;
import maersk.com.mq.KafkaProducer;
import maersk.com.mq.SendToKafkaTask;

@Component
public class MQConsumerListener implements Runnable {

	private Logger log = Logger.getLogger(this.getClass());
	private Thread worker;
	private AtomicBoolean running = new AtomicBoolean(false);

	private int maxAttempts;	

	private MQConnection conn;
	public void setConnection(MQConnection val, int maxAttempts) {
		this.conn = val;
		this.maxAttempts = maxAttempts;
	}
	
	private KafkaTemplate<String, String> kafkaTemplate;
	public void setKafkaTemplate(KafkaTemplate<?,?> val) {
		this.kafkaTemplate = (KafkaTemplate<String, String>) val;
	}

	private String topicName;
	public void setTopicName(String val) {
		this.topicName = val;
	}

    private boolean _debug = false;
	public void setDebug(boolean val) {
		this._debug = val;
	}

	private ThreadPoolExecutor executor;
	private int threadPool;
	public void setThreadPool(int val) {
		this.threadPool = val;
	}
	
	public MQConsumerListener() {
	}
	
	/*
	 * Start the listener thread
	 */
	public void start() {
        worker = new Thread(this);
        worker.start();
    }
	
	/*
	 * Method to interupt then thread
	 */
	public synchronized void interrupt() {
        running.set(false);
        worker.interrupt();
    }

	/*
	 * Are we running ?
	 */
	private synchronized boolean isRunning() {
        return running.get();
    }
 	
	/*
	 * 
	 * Main loop for processing MQ messages ....
	 * 
	 * set GetMessageOptions, and read messages from the queue ..
	 * 
	 */
	@Override
	public void run() {
	
		if (this._debug) { log.info("Starting MQConsumerListener " ); }		
		if (this._debug) { log.info("Creating " + this.threadPool + " fixed threadpools" ); }		
		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(this.threadPool);
		
		running.set(true);
		MQMessage msg = null;
		while (running.get()) {
	
            try { 
    			try {
    				if (this.conn.isConnected()) {
	    				msg = this.conn.getMessage();    	
	    				
	    				/*
	    				 * If we have process the message 'x' number of time and it is now
	    				 * ... over the threshhold, forget it and put it to the backout queue
	    				 */
	    				if (msg != null) {
	    					this.conn.setBackoutQueueDetails();

	    					if (msg.backoutCount > this.conn.getBackoutThreshhold()) {
	    						writeMessageToBackoutQueue(msg);
	    					
	    					} else {
	    						sendAsyncToKafka(msg);
	    						if (this._debug) { log.info(">>>>>>>>>>>>>>>>> message sent"); }
	    					}
	    				}
	    				msg = null;
    				}
    				
    			} catch (MQException  e) {
    				if (e.completionCode == 2 && 
    						((e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE)
    						|| (e.reasonCode == MQConstants.MQRC_GET_INHIBITED )))
    				{
    					if (this._debug) {
    						log.info("No messages available : " + e.reasonCode );
    					}
    					
    					// Sleep for 5 seconds if the queue is GET_INHIBITED
    					if (e.reasonCode == MQConstants.MQRC_GET_INHIBITED ) {
    						Thread.sleep(5000);
    					}
    				} else {

						// Attempt to reconnect to the queue manager
    					if (e.completionCode == 2 && 
    							((e.reasonCode == MQConstants.MQRC_CONNECTION_BROKEN)
    							|| (e.reasonCode == MQConstants.MQRC_CONNECTION_QUIESCING))) {

    						attemptToReconnect();
    						
    					} else {
    						/*
    						 * If the error is anything but 2195 (Unexpected), then try to capture it
    						 * ... otherwise, we are most likely stopping
    						 */
    						if (e.reasonCode != MQConstants.MQRC_UNEXPECTED_ERROR) {
	    						log.error("Unhandled MQException : reasonCode " + e.reasonCode );
	    						log.error("Exception : " + e.getMessage() );
	    						System.exit(MQKafkaConstants.EXIT);
	    						}
    						}
    					}
    				}
    				
    			} catch (Exception e) {
    				log.error("Unhandled Exception procesing MQ messages : " + e.getMessage() );
    				if (this.conn.isConnected()) {
						if (msg != null) {
							try {
								rollBack();
								
							} catch (MQException e1) {
							}
						}
					}
    			}
         } 		
	}
	
	/*
	 * Try to reconnect to the queue manager
	 */
	private void attemptToReconnect()  {

		int maxAttempts = this.maxAttempts;
		int attempts = MQKafkaConstants.REPROCESS_MSG_INIT;
		
		while (attempts <= maxAttempts) {
			try {
				Thread.sleep(5000);			
				this.conn.reConnectToTheQueueManager();
				break;
				
			} catch (Exception e) {
				if (e instanceof MQException) {
					log.warn("Reattempting to connect to queue manager : attempt no : " + attempts);
				}
				attempts++;

			}
		}
	}

	/*
	 * Send messages async to Kafka 
	 */
	private void sendAsyncToKafka(MQMessage msg) throws MQException {
		
		try {
			sendMessageToKafka(msg);
			
		} catch (IOException | MQException | InterruptedException | ExecutionException e) {
			log.error("Unable to successfully process async request");
			rollBack();
		}
	}
	
	/*
	 * Send the message to Kafka ...
	 * 
	 */
	private void sendMessageToKafka(MQMessage msg) throws IOException, MQException, InterruptedException, ExecutionException {
		byte[] message = new byte[msg.getMessageLength()];
		msg.readFully(message);    				

		String payload = new String(message);
		if (_debug) { log.info("msg : " + payload); }
		
		// https://stackoverflow.com/questions/23681822/using-spring-4-0s-new-listenablefuture-with-callbacks-odd-results
		
		//Try getting the RFH2 details from the properties on the MQmessage
		// ... probably not the best way, but works for this ..
		Map rfh2 = getRFHProperties(msg);
		SendToKafkaTask sendToKafkaTask = new SendToKafkaTask(payload);
		sendToKafkaTask.setKafkaTemplate(this.kafkaTemplate);
		sendToKafkaTask.setMQConnection(this.conn);
		sendToKafkaTask.setTopicName(this.topicName);
		sendToKafkaTask.setRFH2Headers(rfh2);
		
		this.executor.submit(sendToKafkaTask);
		
		if (this._debug) { log.info("************* message is being processed *********************"); }
		
	}
	
	/*
	 * Get the RFH2 details
	 */
	private Map<String,String> getRFHProperties(MQMessage msg) {
		
		Map<String,String> rfh2 = new HashMap<String, String>();
		
		String rfh2TopicName= null;
		try {
			rfh2TopicName = msg.getStringProperty("usr.source-topic");
			rfh2.put("source-topic", rfh2TopicName.trim());
			
		} catch (Exception e) {	
			if (this._debug) { log.info("source-topic properties does not exist in the MQRFH2"); }
		}

		String rfh2Key = null;
		try {
			rfh2Key = msg.getStringProperty("usr.key");
			rfh2.put("key", rfh2Key.trim());
		
		} catch (Exception e) {	
			if (this._debug) { log.info("key properties does not exist in the MQRFH2"); }
		}
		
		return rfh2;
		
	}

	/*
	 * Message was successfully sent to Kafka, so commit the messages from the queue
	 */
	protected synchronized void successfullSend() throws MQException {
		if (_debug) { log.info("message successfully sent " ); }
		this.conn.commit();
	}
	
		
	/*
	 * failure, rollback
	 */	
	protected synchronized void rollBack() throws MQException {
		this.conn.rollBack();
	}

	/*
	 * Send to BOQ
	 */
	private synchronized void writeMessageToBackoutQueue(MQMessage msg) throws MQDataException, IOException, MQException {
		if (_debug) { log.error("Attempting to write message to the backout queue" ); }	
		this.conn.writeMessageToBackoutQueue(msg);
		this.conn.commit();
		
	}

	/*
	 * Send to DLQ
	 */
	private synchronized void sendToDLQ(MQMessage msg) throws MQDataException, IOException, MQException {
		if (_debug) { log.error("Attempting to write messages to the DLQ" ); }	
		this.conn.writeMessageToDLQ(msg);
		this.conn.commit();
		
	}
	
}
