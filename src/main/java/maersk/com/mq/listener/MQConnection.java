package maersk.com.mq.listener;

import java.io.IOException;
import java.net.URL;
import java.util.Hashtable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDLH;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQHeaderList;
import com.ibm.mq.headers.MQRFH2;

import maersk.com.kafka.constants.MQKafkaConstants;
import maersk.com.mq.KafkaProducer;


@Component
public class MQConnection implements ApplicationListener<ContextRefreshedEvent> {

	private Logger log = Logger.getLogger(this.getClass());

	@Value("${application.debug:false}")
    private boolean _debug;
	
	@Value("${ibm.mq.queuemanager}")
	private String queueManager;
	public String GetQueueManagerName() { return this.queueManager; }
	public void SetQueueManagerName(String val) { this.queueManager = val; }
	
	// taken from connName
	private String hostName;
	public String GetHostName() { return this.hostName; }
	public void SetHostName(String val) { this.hostName = val; }
	
	// hostname(port)
	@Value("${ibm.mq.connName}")
	private String connName;	
	public String GetConnName() { return this.connName; }
	public void SetConnName(String val) { this.connName = val; }
	
	@Value("${ibm.mq.channel}")
	private String channel;
	public String GetChannel() { return this.channel; }
	public void SetChannel(String val) { this.channel = val; }
	
	@Value("${ibm.mq.queue}")
	private String srcQueue;
	
	private int port;
	public int GetPort() { return this.port; }
	public void SetPort(int val) { this.port = val; }
	
	@Value("${ibm.mq.useCCDT:false}")
	private boolean useCCDT;
	public boolean GetCCDT() { return this.useCCDT; }
	public void SetCCDT(boolean val) { this.useCCDT = val; }

	@Value("${ibm.mq.ccdtFile:missing}")
	private String ccdtFile;

	@Value("${ibm.mq.user}")
	private String userId;
	public String GetUserId() { return this.userId; }
	public void SetUserId(String val) { this.userId = val; }
	
	@Value("${ibm.mq.password}")
	private String password;
	public String GetPassword() { return this.password; }
	public void SetPassword(String val) { this.password = val; }
	
	
	@Value("${ibm.mq.sslCipherSpec}")
	private String cipher;
	@Value("${ibm.mq.waitInterval:5000}")
	private int waitInterval;
    @Value("${ibm.mq.retries.maxAttempts:3}")
	private int maxAttempts;	
	//
	@Value("${ibm.mq.useSSL}")
	private boolean bUseSSL;
	
	@Value("${ibm.mq.security.truststore}")
	private String truststore;
	@Value("${ibm.mq.security.truststore-password}")
	private String truststorepass;
	@Value("${ibm.mq.security.keystore}")
	private String keystore;
	@Value("${ibm.mq.security.keystore-password}")
	private String keystorepass;
	
    @Value("${ibm.mq.mqmd.rfh2.include:false}")
	private boolean includeRFH2;  
	
	@Value("${kafka.dest.topic}")
	private String topicName;

	@Value("${kafka.dest.threadpool:1}")
	private int threadPool;
	
	private String dlqName;
	
	private static MQQueue dlqQueue = null;
	private static MQQueue boqQueue = null;
	
	private MQQueue queue;
	private MQQueueManager queManager;
	public MQQueueManager getQueueManager() {
		return this.queManager;
	}
	
	private MQGetMessageOptions gmo;
	private MQConsumerListener listener;
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private int backoutThreashhold;
	public int getBackoutThreshhold() {
		return this.backoutThreashhold;
	}
	private String backoutQueue;
	public String getBackoutQueue() {
		return this.backoutQueue;
	}
	

	public MQConnection() {
	}

	public boolean isConnected() {
		return this.queManager.isConnected();
	}
	/*
	 * Called from MQConsumerListener to reconnect to the queue manager
	 */
	public void reConnectToTheQueueManager() throws MQException, MQDataException, Exception {
		this.queManager = createQueueManagerConnection();
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

		
		return queManager;
	}

	@Bean("getmessageoptions")
	@DependsOn("queuemanager")
	public MQGetMessageOptions CreateGetMessageOptions() throws MQException {

		log.info("Creating get message options");

		this.gmo = new MQGetMessageOptions();
		this.gmo.options = MQConstants.MQGMO_WAIT 
				+ MQConstants.MQGMO_FAIL_IF_QUIESCING 
				+ MQConstants.MQGMO_CONVERT
				+ MQConstants.MQGMO_SYNCPOINT
				+ MQConstants.MQGMO_PROPERTIES_IN_HANDLE;
		/*
		 * if we want to process the MQRFH2 header
		 */
		if (this.includeRFH2) {
				this.gmo.options += MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2;
		}
		
		// wait until we get something
		this.gmo.waitInterval = this.waitInterval;
		return this.gmo;
		
	}

	@Bean("deadletterandopenqueue")
	@DependsOn({"getmessageoptions","queuemanager"})
	public MQQueue GetDeadLetterQueueAndOpenQueueForReading() throws MQException {

		log.info("Getting DLQ and Opeing queue for reading");

		this.dlqName = this.queManager.getAttributeString(MQConstants.MQCA_DEAD_LETTER_Q_NAME, 48).trim();
		this.queue = openQueueForReading(this.srcQueue);
		return this.queue;
		
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
	 * Try and get the BOQ details from the queue that is being read
	 * ... there does not have to be a BOQ configured to the queue
	 */
	public void setBackoutQueueDetails() {
		getBackoutQueueDetails(this.queue);
	}
	
	/*
	 * Get the BOQ name and BOQ threshhold value
	 */
	private void getBackoutQueueDetails(MQQueue queue) {

		// Get backout queue and threshold values
		int[] query = {MQConstants.MQIA_BACKOUT_THRESHOLD, MQConstants.MQCA_BACKOUT_REQ_Q_NAME };
		int[] outi = new int[1];
		byte[] outb = new byte[48];
		
		try {
			if (queue.getQueueType() == MQConstants.MQQT_ALIAS) {
				int[] basequery = {MQConstants.MQCA_BASE_Q_NAME };
				int[] baseouti = new int[1];
				byte[] baseoutb = new byte[48];
				queue.inquire(basequery, baseouti, baseoutb);
				String queueName = new String(baseoutb).trim();
				
				MQQueue basequeue = openQueueForReading(queueName);
				basequeue.inquire(query, outi, outb);
				basequeue.close();
				
			} else {
				queue.inquire(query, outi, outb);
				
			}

			this.backoutThreashhold = outi[0];
			this.backoutQueue = new String(outb).trim();

		} catch (MQException e) {
			this.backoutQueue = null;
			this.backoutThreashhold = 1;
		}
		
		
	}

	/*
	 * Open a queue for reading
	 */
	public MQQueue openQueueForReading(String qName) {
		
		if (this._debug) { log.info("Opening queue " + qName + " for reading"); }
		
		MQQueue inQueue = null;
		int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
				+ MQConstants.MQOO_INQUIRE 
				+ MQConstants.MQOO_INPUT_SHARED;

		try {
			inQueue = this.queManager.accessQueue(qName, openOptions);
			if (this._debug) { log.info("Queue : " + qName + " opened"); }
			
		} catch (MQException e) {
			log.error("Unable to open queue : " + qName);
			log.error("Message : " + e.getMessage() );
			System.exit(MQKafkaConstants.EXIT);
		}
			
		return inQueue;
		
	}
	
	/*
	 * Open a queue for writing
	 */
	public MQQueue openQueueForWriting(String queueName) throws MQException {

		MQQueue queue = null;
		int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
				+ MQConstants.MQOO_OUTPUT ;
		queue = this.queManager.accessQueue(queueName, openOptions);
		return queue;
	}
	
	public MQMessage getMessage() throws MQException {
		MQMessage msg = new MQMessage();
		this.queue.get(msg, this.gmo);
		return msg;
	}
	
	public void closeQueue() {
		try {
			this.boqQueue.close();
		} catch (Exception e) {
			//
		}
		this.boqQueue = null;
	}

	public void begin() throws MQException {
		this.queManager.begin();
	}
	
	public void commit() throws MQException {
		this.queManager.commit();
	}

	public void rollBack() throws MQException {
		this.queManager.backout();
	}

	/*
	 * Write the BOQ
	 */
	public void writeMessageToBackoutQueue(MQMessage message) throws MQDataException, IOException{

		MQPutMessageOptions pmo = new MQPutMessageOptions();	
		pmo.options = MQConstants.MQPMO_NEW_MSG_ID + MQConstants.MQPMO_FAIL_IF_QUIESCING;
		message.expiry = MQKafkaConstants.UNLIMITED_EXPIRY;
		//MQQueue boqQueue = null;
		try {
			if (this.boqQueue == null) { 
				this.boqQueue = openQueueForWriting(this.backoutQueue);
			}
			this.boqQueue.put(message,pmo);
			log.warn("Message written to BackoutQueue: " + this.backoutQueue);
			
		} catch (MQException e) {
			log.error("Error writting to BOQ " + this.backoutQueue);
			log.error("Reason : " + e.reasonCode + " Description : " + e.getMessage());			
			writeMessageToDLQ(message);
			
		} catch (Exception e) {
			log.error("Error writting to BOQ " + this.backoutQueue);
			log.error("Description : " + e.getMessage());
			writeMessageToDLQ(message);
			
		} finally {
			closeQueue();
		}

	}
	
	/*
	 * if we get an error writing to a queue, try writing it to the DLQ
	 */
	public void writeMessageToDLQ(MQMessage message) throws MQDataException, IOException {

		MQPutMessageOptions pmo = new MQPutMessageOptions();	
		pmo.options = MQConstants.MQPMO_NEW_MSG_ID + MQConstants.MQPMO_FAIL_IF_QUIESCING;
		message.expiry = MQKafkaConstants.UNLIMITED_EXPIRY;

		try {
			if (this.dlqQueue == null) {
				this.dlqQueue = openQueueForWriting(this.dlqName);
			}
			this.dlqQueue.put(message,pmo);
			log.warn("Message written to DLQ : " + this.dlqName);
			
		} catch (MQException e) {
			log.error("Error writting to DLQ " + this.dlqName);
			log.error("Reason : " + e.reasonCode + " Description : " + e.getMessage());			
						
		} catch (Exception e) {
			log.error("Error writting to DLQ " + this.dlqName);
			log.error("Description : " + e.getMessage());

		}
		
	}
	
	/*
	 * Create an MQConnection listener object to process MQ messages
	 */
	protected void createMQListenerObject() {
		if (this._debug) {log.info("Creating MQ Listener object ...."); }
		this.listener = new MQConsumerListener();
		this.listener.setConnection(this, this.maxAttempts);
		this.listener.setKafkaTemplate(this.kafkaTemplate);
		this.listener.setTopicName(this.topicName);
		this.listener.setDebug(this._debug);
		this.listener.setThreadPool(this.threadPool);
		this.listener.start();
		if (this._debug) {log.info("MQ Listener started ...."); }
		
	}
	
	
	/*
	 * Extract connection server and port
	 */
	public void validateHostAndPort() {
		validateHostAndPort(this.useCCDT, this.connName);
	}
	
	public void validateHostAndPort(boolean useCCDT, String conn) {

		/*
		 * ALL parameter are passed in the application.yaml file ...
		 *    These values can be overridden using an application-???.yaml file per environment
		 *    ... or passed in on the command line
		 */
		if (useCCDT && (!conn.equals(""))) {
			log.error("The use of MQ CCDT filename and connName are mutually exclusive");
			System.exit(MQKafkaConstants.EXIT);
		}
		if (useCCDT) {
			return;
		}

		// Split the host and port number from the connName ... host(port)
		if (!conn.equals("")) {
			Pattern pattern = Pattern.compile("^([^()]*)\\(([^()]*)\\)(.*)$");
			Matcher matcher = pattern.matcher(conn);	
			if (matcher.matches()) {
				this.hostName = matcher.group(1).trim();
				this.port = Integer.parseInt(matcher.group(2).trim());
			} else {
				log.error("While attempting to connect to a queue manager, the connName is invalid ");
				System.exit(MQKafkaConstants.EXIT);				
			}
		} else {
			log.error("While attempting to connect to a queue manager, the connName is missing  ");
			System.exit(MQKafkaConstants.EXIT);
			
		}

	}
	
	/*
	 * Check the user, if its in the configuration file, pass it in 
	 */
	public void validateUser() {
		validateUser(this.userId);
		
	}
	
	private void validateUser(String userId) {

		// if no use, for get it ...
		if (userId == null) {
			return;
		}
		
		if (!userId.equals("")) {
			if ((userId.equals("mqm") || (userId.equals("MQM")))) {
				log.error("The MQ channel USERID must not be running as 'mqm' ");
				System.exit(MQKafkaConstants.EXIT);
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
