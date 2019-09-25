package maersk.com.kafka.constants;

import org.apache.log4j.Logger;

public interface MQKafkaConstants {

		public static int EXIT = 1;

		public static int MQMD_EXPIRY_UNLIMITED = 0;
		public static int MQMD_EXPIRY_LOW = 30;
		public static int MQMD_EXPIRY_MEDIUM = 100;
		
		public static int REPROCESS_MSG_INIT = 1;
		
		public static int COMPCODE = 2;
		public static int UNLIMITED_EXPIRY = -1;
		
		
}
