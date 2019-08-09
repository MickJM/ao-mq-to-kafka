package maersk.com.mq.listener;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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

@Component
public class MQConsumerListener implements Runnable {

	private Logger log = Logger.getLogger(this.getClass());
	private Thread worker;
	private AtomicBoolean running = new AtomicBoolean(false);

	private int interval;

	private MQConnection conn;
	public void setConnection(MQConnection val) {
		this.conn = val;
	}
	
	private MQQueueManager queManager;
	public void setQueueManager(MQQueueManager val) {
		this.queManager = val;
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

	private MQQueue queue;
	//private MQGetMessageOptions gmo;
	

	//public MQConsumerListener(MQConnection conn, MQQueueManager qm, MQQueue q, KafkaTemplate<String, String> kt) {

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
	 * Are we running 
	 */
	private synchronized boolean isRunning() {
        return running.get();
    //    return running1;
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
	
		running.set(true);

		if (this._debug) { log.info("Starting MQConsumerListener " ); }

		MQGetMessageOptions gmo = new MQGetMessageOptions();
		gmo.options = MQConstants.MQGMO_WAIT 
				+ MQConstants.MQGMO_FAIL_IF_QUIESCING 
				+ MQConstants.MQGMO_CONVERT
				+ MQConstants.MQGMO_SYNCPOINT
				+ MQConstants.MQGMO_PROPERTIES_IN_HANDLE;
		
		// wait until we get something
		gmo.waitInterval = 5000;

		/*
		try {
			AttemptToReconnect();
		
		} catch (MQException | MQDataException e1) {
			log.error("Unable to connect to queue manager ");
			e1.printStackTrace();
		}
		*/
		
		// Open the queue for reading
		this.queue = this.conn.OpenQueueForReading();
		MQMessage msg = null;
		
		//while (isRunning()) {
		while (running.get()) {
	
            try { 

    			try {
    				
    				msg = new MQMessage();
    				this.queue.get(msg, gmo);
    				
    				if (msg != null) {
    					SendMessageToKafka(msg);
    					//this.queManager.commit();
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

    						AttemptToReconnect();
    						
    					} else {
    						log.error("Unhandled MQException : reasonCode " + e.reasonCode );
    						log.error("Exception : " + e.getMessage() );
    						System.exit(1);
    					}
    				}
    				
    			} catch (Exception e) {
    				log.error("Unhandled Exception procesing MQ messages : " + e.getMessage() );
    				System.exit(1);
    			}

            } catch (Exception e) {
                Thread.currentThread().interrupt();
                log.error("Thread was interrupted, Failed to complete operation");
                log.error("Exception: " + e.getMessage());
                
            }
         } 		
	}
	
	/*
	 * Try to reconnect to the queue manager
	 */
	private void AttemptToReconnect()  {

		int maxAttempts = 3;
		int attempts = 1;
		
		while (attempts <= maxAttempts) {
			try {
				Thread.sleep(5000);			
				this.queManager = this.conn.reConnectToTheQueueManager();
				this.queue = this.conn.OpenQueueForReading();
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
	 * Send the message to Kafka ...
	 * 
	 */
	private void SendMessageToKafka(MQMessage msg) throws IOException {
		
		byte[] message = new byte[msg.getMessageLength()];
		msg.readFully(message);    				

		String payload = new String(message);
		log.info("msg : " + payload);
		
		ListenableFuture<SendResult<String,String>> future =
				kafkaTemplate.send(this.topicName, payload);

		future.addCallback(new ListenableFutureCallback<SendResult<String,String>>() {
			
			@Override
			public void onSuccess(SendResult<String,String> sendResult) {
		
				if (_debug) { log.info("message successfully sent " ); }
				
				try {
					queManager.commit();
					
				} catch (MQException e) {
					log.error("Unable to commit transaction to MQ after successfully sending messages to Kafka");
				}
				
			}

			@Override
			public void onFailure(Throwable ex) {
			
				processFailures(ex);
				System.exit(1);
			}
		});
		
		
//		this.kafkaTemplate.send("mmo275topic", msg);
	}
	
	
	/*
	 * Called from ListenableFutureCallback
	 */
	protected void processFailures(Throwable ex) {

		log.error("Unable to send message to Kafka : " + ex.getMessage());
		
	}


	
}
