package callback;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zookeeper.ZkEventTypes;
import zookeeper.ZkKeeperStates;

/**
 * Created by weizhenyi on 2017/6/25.
 */
public class DefaultWatcherCallBack implements WatcherCallBack{

	private static Logger LOG = LoggerFactory.getLogger(DefaultWatcherCallBack.class);

	@Override
	public void execute(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path) {
		System.out.println("Zookeeper state update:" + ZkKeeperStates.getStateName(state) + ", " + ZkEventTypes.getStateName(type) + ", " + path);
		LOG.info("Zookeeper state update:" + ZkKeeperStates.getStateName(state) + "," + ZkEventTypes.getStateName(type) + "," + path);
	}
}
