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

import maersk.com.mq.KafkaProducer;

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
	
	//private MQQueueManager queManager;
	//public void setQueueManager(MQQueueManager val) {
	//	this.queManager = val;
	//}

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

	//private MQQueue queue;
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
		running.set(true);

		/*
		MQGetMessageOptions gmo = new MQGetMessageOptions();
		gmo.options = MQConstants.MQGMO_WAIT 
				+ MQConstants.MQGMO_FAIL_IF_QUIESCING 
				+ MQConstants.MQGMO_CONVERT
				+ MQConstants.MQGMO_SYNCPOINT
				+ MQConstants.MQGMO_PROPERTIES_IN_HANDLE;
		
		// wait until we get something
		gmo.waitInterval = 5000;
		*/
		
		/*
		try {
			AttemptToReconnect();
		
		} catch (MQException | MQDataException e1) {
			log.error("Unable to connect to queue manager ");
			e1.printStackTrace();
		}
		*/
		
		// Open the queue for reading
		//this.queue = this.conn.openQueueForReading();
		//MQMessage msg = null;
		MQMessage msg = null;
		//while (isRunning()) {
		while (running.get()) {
	
            try { 
    			try {
    				msg = this.conn.getMessage();    				
    				if (msg != null) {
    					sendMessageToKafka(msg);
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
    						log.error("Unhandled MQException : reasonCode " + e.reasonCode );
    						log.error("Exception : " + e.getMessage() );
    						sendToDLQ(msg);
    						//System.exit(1);
    					}
    				}
    				
    			} catch (Exception e) {
    				log.error("Unhandled Exception procesing MQ messages : " + e.getMessage() );
    				sendToDLQ(msg);
    				//System.exit(1);
    			}

            } catch (Exception e) {
                Thread.currentThread().interrupt();
                log.error("Thread was interrupted, Failed to complete operation");
                log.error("Exception: " + e.getMessage());
				try {
					sendToDLQ(msg);
					
				} catch (MQDataException e1) {
					e1.printStackTrace();
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (MQException e1) {
					e1.printStackTrace();
				}
            }
         } 		
	}
	
	/*
	 * Try to reconnect to the queue manager
	 */
	private void attemptToReconnect()  {

		int maxAttempts = 3;
		int attempts = 1;
		
		while (attempts <= maxAttempts) {
			try {
				Thread.sleep(5000);			
				this.conn.reConnectToTheQueueManager();
				//this.queue = this.conn.openQueueForReading();
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
	private void sendMessageToKafka(MQMessage msg) throws IOException {
		byte[] message = new byte[msg.getMessageLength()];
		msg.readFully(message);    				

		String payload = new String(message);
		if (_debug) { log.info("msg : " + payload); }
		
		ListenableFuture<SendResult<String,String>> future =
				kafkaTemplate.send(this.topicName, payload);

		future.addCallback(new ListenableFutureCallback<SendResult<String,String>>() {
			
			@Override
			public void onSuccess(SendResult<String,String> sendResult) {		
				successfullSend();
				
			}

			@Override
			public void onFailure(Throwable ex) {
			
				try {
					processFailures(ex, msg);

				} catch (MQException e) {
					log.warn("Unable to commit message to DLQ : reasonCode " + e.reasonCode );
					log.warn("Message : " + e.getMessage() );
	
				} catch (MQDataException e) {
					log.warn("Unable to commit message to DLQ : reasonCode " + e.reasonCode );
					log.warn("Message : " + e.getMessage() );
					
				} catch (IOException e) {
					log.warn("Unable to commit message to DLQ " );
					log.warn("Message : " + e.getMessage() );
				
				}
			}
		});
		
	}
	

	/*
	 * Messages was successfully sent to Kafka, so commit the messages from the queue
	 */
	protected void successfullSend() {
		if (_debug) { log.info("message successfully sent " ); }
		try {
			this.conn.commit();
			
		} catch (MQException e) {
			log.error("Unable to commit transaction to MQ after successfully sending messages to Kafka");
		}
	}
	
	/*
	 * Called from ListenableFutureCallback
	 * ... send to DLQ
	 */
	protected void processFailures(Throwable ex, MQMessage msg) throws MQDataException, IOException, MQException {
		if (_debug) { log.error("Unable to send message to Kafka : " + ex.getMessage()); }
		sendToDLQ(msg);
	}

	/*
	 * Send to DLQ
	 */
	private void sendToDLQ(MQMessage msg) throws MQDataException, IOException, MQException {
		this.conn.WriteMessageToDLQ(msg);
		this.conn.commit();
		
	}
	
}
