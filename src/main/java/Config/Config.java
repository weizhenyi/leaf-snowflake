package Config;

import java.io.File;
import java.util.Map;

/**
 * Created by weizhenyi on 2017/6/25.
 */
public class Config {

	public static final String FILE_SEPERATEOR = File.separator;

	public static final String LEAF_LOCAL_DIR = "leaf.local.dir";
	public static final Object LEAF_LOCAL_DIR_SCHEMA = String.class;

	public static final String LEAF_RPC_PORT = "leaf.rpc.port";
	public static final Object LEAF_RPC_PORT_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 *  Zookeeper servers hosts
	 */
	public static final String LEAF_ZOOKEEPER_SERVERS = "leaf.zookeeper.servers";
	public static final Object LEAF_ZOOKEEPER_SERVERS_SCHEMA = ConfigValidation.StringsValidator;

	/**
	 * The port Leaf will use to connect to each of the ZooKeeper servers.
	 */
	public static final String LEAF_ZOOKEEPER_PORT = "leaf.zookeeper.port";
	public static final Object LEAF_ZOOKEEPER_PORT_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 * The root location at which Leaf stores data in ZooKeeper.
	 */
	public static final String LEAF_ZOOKEEPER_ROOT = "leaf.zookeeper.root";
	public static final Object LEAF_ZOOKEEPER_ROOT_SCHEMA = String.class;

	/**
	 * The  node under the leaf.zookeeper.root node  which Leaf stores persistent data in .
	 */
	public static final String LEAF_ZOOKEEPER_FOREVER = "leaf.zookeeper.forever";
	public static final Object LEAF_ZOOKEEPER_FOREVER_SCHEMA = String.class;


	/**
	 * The  node under the leaf.zookeeper.root node  which Leaf stores  ephemeral data in .
	 */
	public static final String LEAF_ZOOKEEPER_EPHEMERAL = "leaf.zookeeper.ephemeral";
	public static final Object LEAF_ZOOKEEPER_EPHEMERAL_SCHEMA = String.class;

	/**
	 * The  leaf heartbeat interval .
	 */
	public static final String LEAF_HEARTBEAT_INTERVAL = "leaf.heartbeat.interval";
	public static final Object LEAF_HEARTBEAT_INTERVAL_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 * The  average timestamps threshold of all peer servers to determine that the current server is mismatched of system clock .
	 */
	public static final String LEAF_AVERAGE_TIMESTAMP_THRESHOLD = "leaf.average.timestamp.threshold";
	public static final Object LEAF_AVERAGE_TIMESTAMP_THRESHOLD_SCHEMA = ConfigValidation.IntegerValidator;


	/**
	 * The connection timeout for clients to ZooKeeper.
	 */
	public static final String LEAF_ZOOKEEPER_CONNECTION_TIMEOUT = "leaf.zookeeper.connection.timeout";
	public static final Object LEAF_ZOOKEEPER_CONNECTION_TIMEOUT_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 * The session timeout for clients to ZooKeeper.
	 */
	public static final String LEAF_ZOOKEEPER_SESSION_TIMEOUT = "leaf.zookeeper.session.timeout";
	public static final Object LEAF_ZOOKEEPER_SESSION_TIMEOUT_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 * The interval between retries of a Zookeeper operation.
	 */
	public static final String LEAF_ZOOKEEPER_RETRY_INTERVAL = "leaf.zookeeper.retry.interval";
	public static final Object LEAF_ZOOKEEPER_RETRY_INTERVAL_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 * The ceiling of the interval between retries of a Zookeeper operation.
	 */
	public static final String LEAF_ZOOKEEPER_RETRY_INTERVAL_CEILING = "leaf.zookeeper.retry.intervalceiling.millis";
	public static final Object LEAF_ZOOKEEPER_RETRY_INTERVAL_CEILING_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 * The number of times to retry a Zookeeper operation.
	 */
	public static final String LEAF_ZOOKEEPER_RETRY_TIMES = "leaf.zookeeper.retry.times";
	public static final Object LEAF_ZOOKEEPER_RETRY_TIMES_SCHEMA = ConfigValidation.IntegerValidator;
	// not use for version 0.1
	public static final String CACHE_TIMEOUT_LIST = "cache.timeout.list";
	public static final String LEAF_HOME = "leaf.home";


}
