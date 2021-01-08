package maersk.com.mq;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.headers.MQDataException;

import maersk.com.mq.listener.MQConnection;

//@RunWith(SpringRunner.class)
@SpringBootTest
public class QueueManagerConnectionTest {

	private Logger log = Logger.getLogger(this.getClass());
	
	@Value("${application.debug:false}")
    private boolean _debug;
	
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

	@Before
	public void SetVariables() {
		this.connName = System.getenv("TEST_HOST");
		this.queueManager = System.getenv("TEST_HOST");
		
	}
	
	@Test
	public void ConnectionVariableTest() {	
		assertTrue("Connection is not set", this.connName != null);
	}
	
	@Test
	public void QueueManagerVariableTest() {
		assertTrue("Queue manager is not set", this.queueManager != null);
	}

	@Test
	public void HostAndPortTest() {
		boolean useCCDT = false;

		MQConnection conn = new MQConnection();
		conn.validateHostAndPort(useCCDT, this.connName);
		
		String server = conn.GetHostName();
		int port = conn.GetPort();
		
		assertTrue("Server name is not valid", server != null);
		assertFalse("Port is not valid", port == 0);
		
	}
	
	@Test
	public void ConnectToQueueManagerTest() {
		boolean useCCDT = false;
		
		MQConnection conn = new MQConnection();
		conn.SetConnName(System.getenv("TEST_CONNAME"));
		conn.validateHostAndPort();
		conn.SetUserId(System.getenv("TEST_USERID"));
		conn.SetPassword(System.getenv("TEST_PASSWORD"));
		conn.SetChannel(System.getenv("TEST_CHANNEL"));
		conn.SetQueueManagerName(System.getenv("TEST_QMGR"));

		MQQueueManager qm = null;
		try {
			qm = conn.createQueueManager();

			/*
		} catch (MQException e) {
			log.info("Error: " + e.getMessage());
			
		} catch (MQDataException e) {
			log.info("Error: " + e.getMessage());
			*/
		} catch (Exception e) {
			log.info("Error: " + e.getMessage());
			
		}
		
		assertTrue("Queue manager failed", qm != null);
		assertTrue("Not connected to queue manager", qm.isConnected());
		
		conn.closeQMConnection();
		
	}
	
	
}
