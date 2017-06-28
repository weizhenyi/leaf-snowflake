package zookeeper;

/**
 * Created by weizhenyi on 2017/6/28.
 */

import Config.Config;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import java.util.List;
import java.util.Map;

public class ZkTool {
	private static Logger LOG = LoggerFactory.getLogger(ZkTool.class);

	public static final String READ_CMD = "read";

	public static final String LIST_CMD = "list";

	public static void usage() {
		LOG.info("Read ZK node's data, please do as following:");
		LOG.info(ZkTool.class.getName() + " read zkpath");

		LOG.info("\nlist subdirectory of zkPath , please do as following:");
		LOG.info(ZkTool.class.getName() + " list zkpath");
	}

	public static String getData(DistributedClusterStat zkClusterState,
								 String path) throws Exception {
		byte[] data = zkClusterState.get_data(path, false);
		if (data == null || data.length == 0) {
			return null;
		}
		return Long.valueOf(Utils.bytesTolong(data)).toString();
	}


	public static void list(String path) {
		DistributedClusterStat zkClusterState = null;

		try {
			conf.put(Config.LEAF_ZOOKEEPER_ROOT , "/");

			zkClusterState = new DistributedClusterStat(conf);

			List<String> children = zkClusterState.get_childern(path, false);
			if (children == null || children.isEmpty() ) {
				LOG.info("No children of " + path);
			}
			else
			{
				StringBuilder sb = new StringBuilder();
				sb.append("Zk node children of " + path + "\n");
				for (String str : children){
					sb.append(" " + str + ",");
				}
				sb.append("\n");
				LOG.info(sb.toString());
			}
		} catch (Exception e) {
			if (zkClusterState == null) {
				LOG.error("Failed to connect ZK ", e);
			} else {
				LOG.error("Failed to list children of  " + path + "\n", e);
			}
		} finally {
			if (zkClusterState != null) {
				zkClusterState.close();
			}
		}
	}


	public static void readData(String path) {

		DistributedClusterStat zkClusterState = null;

		try {
			conf.put(Config.LEAF_ZOOKEEPER_ROOT, "/");

			zkClusterState = new DistributedClusterStat(conf);

			String data = getData(zkClusterState, path);
			if (data == null) {
				LOG.info("No data of " + path);
			}

			StringBuilder sb = new StringBuilder();

			sb.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
			sb.append("Zk node " + path + "\n");
			sb.append("Readable data:" + data + "\n");
			sb.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");

			LOG.info(sb.toString());

		} catch (Exception e) {
			if (zkClusterState == null) {
				LOG.error("Failed to connect ZK ", e);
			} else {
				LOG.error("Failed to read data " + path + "\n", e);
			}
		} finally {
			if (zkClusterState != null) {
				zkClusterState.close();
			}
		}
	}

	private static Map conf;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if (args.length < 2) {
			LOG.info("Invalid parameter");
			usage();
			return;
		}

		conf = Utils.readLeafConfig();

		if (args[0].equalsIgnoreCase(READ_CMD)) {
			readData(args[1]);
		} else if (args[0].equalsIgnoreCase(LIST_CMD)) {
			list(args[1]);
		}

	}
}
