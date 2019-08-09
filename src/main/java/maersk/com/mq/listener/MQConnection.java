package maersk.com.mq.listener;

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
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;


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
	
	@Value("${application.exceptions.show:true}")
    private boolean _exceptions;
	
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
	
	private MQQueueManager queManager;
	public MQQueueManager getQueueManager() {
		return this.queManager;
	}
	
	public MQQueueManager reConnectToTheQueueManager() throws MQException, MQDataException {
		MQQueueManager qm = CreateQueueManagerConnection();
		return qm;
	}
	
	private MQGetMessageOptions gmo;

	private MQConsumerListener listener;
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public MQConnection() {
	}

	/*
	 * Create an MQ queue manager object
	 */
	@Bean("queuemanager") 
	public MQQueueManager CreateQueueManagerConnection() throws MQException, MQDataException {
		
		GetEnvironmentVariables();
		
		Hashtable<String, Comparable> env = new Hashtable<String, Comparable>();
		env.put(MQConstants.HOST_NAME_PROPERTY, this.hostName);
		env.put(MQConstants.CHANNEL_PROPERTY, this.channel);
		env.put(MQConstants.PORT_PROPERTY, this.port);
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
			log.info("Host 		: " + this.hostName);
			log.info("Channel 	: " + this.channel);
			log.info("Port 		: " + this.port);
			log.info("Queue Man : " + this.queueManager);
			log.info("User 		: " + this.userId);
			log.info("Password  : **********");
			if (this.bUseSSL) {
				log.info("SSL is enabled ....");
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
		
		log.info("Attempting to connect to queue manager " + this.queueManager);
		this.queManager = new MQQueueManager(this.queueManager, env);
		log.info("Connection to queue manager established ");

		return queManager;
	}

	
	//@Bean 
	//@DependsOn("queuemanager")
	public MQQueue OpenQueueForReading() {
		
		if (this._debug) { log.info("Opening queue " + this.srcQueue + " for writing"); }
		
		MQQueue inQueue = null;
		int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
				+ MQConstants.MQOO_INQUIRE 
				+ MQConstants.MQOO_INPUT_SHARED;

		try {
			inQueue = this.queManager.accessQueue(srcQueue, openOptions);
			if (this._debug) { log.info("Queue : " + this.srcQueue + " opened"); }
			
		} catch (MQException e) {
			log.error("Unable to open queue : " + this.srcQueue);
			log.error("Message : " + e.getMessage() );
			System.exit(1);
		}
			
		return inQueue;
		
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
		this.listener.setQueueManager(this.queManager);
		this.listener.setKafkaTemplate(this.kafkaTemplate);
		this.listener.setTopicName(this.topicName);
		this.listener.setDebug(this._debug);
		this.listener.start();
		
		if (this._debug) {log.info("MQ Listener started ...."); }
		
	}
	
	
	/*
	 * Extract connection server and port
	 */
	private void GetEnvironmentVariables() {
		
		/*
		 * ALL parameter are passed in the application.yaml file ...
		 *    These values can be overridden using an application-???.yaml file per environment
		 *    ... or passed in on the command line
		 */
		
		// Split the host and port number from the connName ... host(port)
		if (!this.connName.equals("")) {
			Pattern pattern = Pattern.compile("^([^()]*)\\(([^()]*)\\)(.*)$");
			Matcher matcher = pattern.matcher(this.connName);	
			if (matcher.matches()) {
				this.hostName = matcher.group(1).trim();
				this.port = Integer.parseInt(matcher.group(2).trim());
			} else {
				if (this._exceptions) { log.error("While attempting to connect to a queue manager, the connName is invalid "); }
				System.exit(1);				
			}
		} else {
			if (this._exceptions) {log.error("While attempting to connect to a queue manager, the connName is missing  "); }
			System.exit(1);
			
		}

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
    public void CloseQMConnection() {
    	
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
