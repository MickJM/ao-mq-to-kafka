package maersk.com.mq.listener;

import java.io.IOException;
import java.net.URL;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;

import maersk.com.mq.KafkaProducer;


@Component
public class MQConnection implements ApplicationListener<ContextRefreshedEvent> {

	private Logger log = Logger.getLogger(this.getClass());

	@Value("${ibm.mq.queuemanager}")
	private String queueManager;
	
	// taken from connName
	private String hostName;

	// hostname(port)
	@Value("${ibm.mq.connName}")
	private String connName;	
	@Value("${ibm.mq.channel}")
	private String channel;
	@Value("${ibm.mq.queue}")
	private String srcQueue;
	
	private int port;
	
	@Value("${ibm.mq.useCCDT:false}")
	private boolean useCCDT;
	@Value("${ibm.mq.ccdtFile:missing}")
	private String ccdtFile;

	@Value("${ibm.mq.user}")
	private String userId;
	@Value("${ibm.mq.password}")
	private String password;
	@Value("${ibm.mq.sslCipherSpec}")
	private String cipher;

	//
	@Value("${ibm.mq.useSSL}")
	private boolean bUseSSL;
	
	@Value("${application.debug:false}")
    private boolean _debug;
	
	@Value("${ibm.mq.security.truststore}")
	private String truststore;
	@Value("${ibm.mq.security.truststore-password}")
	private String truststorepass;
	@Value("${ibm.mq.security.keystore}")
	private String keystore;
	@Value("${ibm.mq.security.keystore-password}")
	private String keystorepass;
	
	@Value("${kafka.dest.topic}")
	private String topicName;
	
	private String dlqName;
	
	private MQQueue queue;
	private MQQueueManager queManager;
	public MQQueueManager getQueueManager() {
		return this.queManager;
	}
	
	
	private MQGetMessageOptions gmo;
	private MQConsumerListener listener;
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public MQConnection() {
	}

	/*
	 * Called from MQConsumerListener to reconnect to the queue manager
	 */
	public void reConnectToTheQueueManager() throws MQException, MQDataException, Exception {
		this.queManager = createQueueManagerConnection();
		//return qm;
	}
	
	/*
	 * Create an MQ queue manager object
	 */
	@Bean("queuemanager") 
	public MQQueueManager createQueueManagerConnection() throws MQException, MQDataException, Exception {
		
		validateHostAndPort();
		validateUser();

		Hashtable<String, Comparable> env = new Hashtable<String, Comparable>();
		if (!this.useCCDT) {
			env.put(MQConstants.HOST_NAME_PROPERTY, this.hostName);
			env.put(MQConstants.CHANNEL_PROPERTY, this.channel);
			env.put(MQConstants.PORT_PROPERTY, this.port);
		}
		env.put(MQConstants.CONNECT_OPTIONS_PROPERTY, MQConstants.MQCNO_RECONNECT);
		
		/*
		 * 
		 * If a username and password is provided, then use it
		 * ... if CHCKCLNT is set to OPTIONAL or RECDADM
		 * ... RECDADM will use the username and password if provided ... if a password is not provided
		 * ...... then the connection is used like OPTIONAL
		 */
		
		if (this.userId != null) {
			env.put(MQConstants.USER_ID_PROPERTY, this.userId); 
		}
		if (this.password != null) {
			env.put(MQConstants.PASSWORD_PROPERTY, this.password);
		}
		env.put(MQConstants.TRANSPORT_PROPERTY,MQConstants.TRANSPORT_MQSERIES);

		if (this._debug) {
			if (this.useCCDT) {
				log.info("Using CCDT: " + this.ccdtFile);
				log.info("Queue Man : " + this.queueManager);
				log.info("User      : " + this.userId);
				log.info("Password  : **********");
				if (this.bUseSSL) {
					log.info("SSL is enabled ....");
				}
	
			} else {
				log.info("Queue Man : " + this.queueManager);
				log.info("Host      : " + this.hostName);
				log.info("Channel   : " + this.channel);
				log.info("Port      : " + this.port);
				log.info("User      : " + this.userId);
				log.info("Password  : **********");
				if (this.bUseSSL) {
					log.info("SSL is enabled ....");
				}
			}
		}
		
		// If SSL is enabled (default)
		if (this.bUseSSL) {
			System.setProperty("javax.net.ssl.trustStore", this.truststore);
	        System.setProperty("javax.net.ssl.trustStorePassword", this.truststorepass);
	        System.setProperty("javax.net.ssl.trustStoreType","JKS");
	        System.setProperty("javax.net.ssl.keyStore", this.keystore);
	        System.setProperty("javax.net.ssl.keyStorePassword", this.keystorepass);
	        System.setProperty("javax.net.ssl.keyStoreType","JKS");
	        System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings","false");
	        env.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, this.cipher); 
		
		} else {
			if (this._debug) {
				log.info("SSL is NOT enabled ....");
			}
		}
		
        //System.setProperty("javax.net.debug","all");
		if (this._debug) {
			log.info("TrustStore       : " + this.truststore);
			log.info("TrustStore Pass  : ********");
			log.info("KeyStore         : " + this.keystore);
			log.info("KeyStore Pass    : ********");
			log.info("Cipher Suite     : " + this.cipher);
		}
		
		if (!this.useCCDT) {
			log.info("Attempting to connect to queue manager " + this.queueManager);
			this.queManager = new MQQueueManager(this.queueManager, env);
			log.info("Connection to queue manager established ");
			
		} else {
			URL ccdtFileName = new URL("file:///" + this.ccdtFile);
			log.info("Attempting to connect to queue manager " + this.queueManager + " using CCDT file");
			this.queManager = new MQQueueManager(this.queueManager, env, ccdtFileName);
			log.info("Connection to queue manager established ");			
		}

		this.gmo = new MQGetMessageOptions();
		this.gmo.options = MQConstants.MQGMO_WAIT 
				+ MQConstants.MQGMO_FAIL_IF_QUIESCING 
				+ MQConstants.MQGMO_CONVERT
				+ MQConstants.MQGMO_SYNCPOINT
				+ MQConstants.MQGMO_PROPERTIES_IN_HANDLE;
		
		// wait until we get something
		this.gmo.waitInterval = 5000;

		this.dlqName = this.queManager.getAttributeString(MQConstants.MQCA_DEAD_LETTER_Q_NAME, 48).trim();
		this.queue = openQueueForReading(this.srcQueue);
		
		return queManager;
	}

	
	//@Bean 
	//@DependsOn("queuemanager")
	public MQQueue openQueueForReading(String qName) {
		
		if (this._debug) { log.info("Opening queue " + qName + " for writing"); }
		
		MQQueue inQueue = null;
		int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
				+ MQConstants.MQOO_INQUIRE 
				+ MQConstants.MQOO_INPUT_SHARED;

		try {
			inQueue = this.queManager.accessQueue(srcQueue, openOptions);
			if (this._debug) { log.info("Queue : " + qName + " opened"); }
			
		} catch (MQException e) {
			log.error("Unable to open queue : " + qName);
			log.error("Message : " + e.getMessage() );
			System.exit(1);
		}
			
		return inQueue;
		
	}
	
	public MQMessage getMessage() throws MQException {
		MQMessage msg = new MQMessage();
		this.queue.get(msg, this.gmo);
		return msg;
	}

	public void commit() throws MQException {
		this.queManager.commit();
	}

	/*
	 * if we get an error writing to a queue, try writing it to the DLQ
	 */
	public void WriteMessageToDLQ(MQMessage message) throws MQDataException, IOException {

		MQPutMessageOptions pmo = new MQPutMessageOptions();	
		pmo.options = MQConstants.MQPMO_NEW_MSG_ID + MQConstants.MQPMO_FAIL_IF_QUIESCING;
		message.expiry = -1;

		MQQueue dlqQueue = null;
		try {
			dlqQueue = openQueueForReading(this.dlqName);	
			dlqQueue.put(message,pmo);
			log.warn("Message written to DLQ");
			
		} catch (MQException e) {
			log.error("Error writting to DLQ " + this.dlqName);
			log.error("Reason : " + e.reasonCode + " Description : " + e.getMessage());			
						
		} catch (Exception e) {
			log.error("Error writting to DLQ " + this.dlqName);
			log.error("Description : " + e.getMessage());

		} finally {
			try {
				if (dlqQueue != null) {
					dlqQueue.close();
				}	
				
			} catch (MQException e) {
				log.warn("Error closing DLQ " + this.dlqName);
			}
		}
		
	}
	
	/*
	 * Override the onApplicationEvent, so we can create an MQ listener when we know that this
	 *    object has been fully created
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {

		createMQListenerObject();
		
	}

	/*
	 * Create an MQConnectionListener object to process MQ messages
	 */
	protected void createMQListenerObject() {

		this.listener = new MQConsumerListener();
		this.listener.setConnection(this);
		//this.listener.setQueueManager(this.queManager);
		this.listener.setKafkaTemplate(this.kafkaTemplate);
		this.listener.setTopicName(this.topicName);
		this.listener.setDebug(this._debug);		
		this.listener.start();
		
		if (this._debug) {log.info("MQ Listener started ...."); }
		
	}
	
	
	/*
	 * Extract connection server and port
	 */
	private void validateHostAndPort() {
		
		/*
		 * ALL parameter are passed in the application.yaml file ...
		 *    These values can be overridden using an application-???.yaml file per environment
		 *    ... or passed in on the command line
		 */
		if (this.useCCDT && (!this.connName.equals(""))) {
			log.error("The use of MQ CCDT filename and connName are mutually exclusive");
			System.exit(1);
		}
		if (this.useCCDT) {
			return;
		}

		// Split the host and port number from the connName ... host(port)
		if (!this.connName.equals("")) {
			Pattern pattern = Pattern.compile("^([^()]*)\\(([^()]*)\\)(.*)$");
			Matcher matcher = pattern.matcher(this.connName);	
			if (matcher.matches()) {
				this.hostName = matcher.group(1).trim();
				this.port = Integer.parseInt(matcher.group(2).trim());
			} else {
				log.error("While attempting to connect to a queue manager, the connName is invalid ");
				System.exit(1);				
			}
		} else {
			log.error("While attempting to connect to a queue manager, the connName is missing  ");
			System.exit(1);
			
		}

	}
	
	/*
	 * Check the user, if its passed in 
	 */
	private void validateUser() {

		// if no use, for get it ...
		if (this.userId == null) {
			return;
		}
		
		if (!this.userId.equals("")) {
			if ((this.userId.equals("mqm") || (this.userId.equals("MQM")))) {
				log.error("The MQ channel USERID must not be running as 'mqm' ");
				System.exit(1);
			}
		} else {
			this.userId = null;
			this.password = null;
		}		
	}
	
	/*
	 * Close any open connections cleanly
	 */
    @PreDestroy
    public void closeQMConnection() {
    	
    	if (this.listener != null) {
    		this.listener.interrupt();
    	}

    	try {
	    	if (this.queManager != null) {	
	    		if (this._debug) {
	    			log.info("Closing queue manager connection");
	    		}
	    		this.queManager.close();
	    	}
	    	
    	} catch (Exception e) {
    		// do nothing
    	}
    }

	
	
}
