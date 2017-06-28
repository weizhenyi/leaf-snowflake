package zookeeper;

/**
 * Created by weizhenyi on 2017/6/25.
 */
import Config.Config;
import callback.WatcherCallBack;
import callback.ClusterStateCallback;
import cache.LeafCache;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PathUtils;
import utils.Utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedClusterStat implements ClusterStat{
	private static Logger LOG = LoggerFactory.getLogger(DistributedClusterStat.class);
	private Zookeeper zkobj = new Zookeeper();
	private CuratorFramework zk;
	private Map<Object,Object> conf;
	private AtomicBoolean active;
	private WatcherCallBack watcher;
	/**
	 * why run all callbacks, when receive one event
	 */
	private ConcurrentHashMap<UUID, ClusterStateCallback> callbacks = new ConcurrentHashMap<UUID, ClusterStateCallback>();
	private LeafCache zkCache;

	public DistributedClusterStat(Map<Object,Object> _conf ) throws Exception
	{
		this.conf = _conf;
		CuratorFramework _zk = mkZk();//创建Zookeeper连接及重试策略
		String path = String.valueOf(conf.get(Config.LEAF_ZOOKEEPER_ROOT));
		zkobj.mkdirs(_zk,path);// 创建一个永久目录
		_zk.close();

		active = new AtomicBoolean(true);



		watcher = new WatcherCallBack() {
			@Override
			public void execute(KeeperState state, EventType type, String path) {
				if ( active.get())
				{
					if(!(state.equals(KeeperState.SyncConnected)))
					{
						LOG.warn("Received event " + state + ": " + type + ": " + path + "with disconnected  from Zookeeper.");
						//System.out.println("Received event " + state + ":" + type + ":" + path + "with disconnected Zookeeper.");
					}
					else
					{
						LOG.info("Received event " + state + ":" + type + ":" + path);
						//System.out.println("Received event " + state + ":" + type + ":" + path);
						if(type.equals(EventType.NodeChildrenChanged)) //leaf 的临时node节点发生了变化（server上线或者下线)
						{
							LOG.info("Node childrens changed at path: " + path);
							//重新注册watcher事件
							try {
								List<String> children = get_childern(path,true);
								LOG.info("children list at path : " + path + " is " + children);
							} catch (Exception e)
							{
								LOG.warn("faild to get children in path: " + path,e);
							}
						}
					}

					if (!type.equals(EventType.None))
					{
						//System.out.println("Received event " + state + ":" + type + ":" + path);
						LOG.info("Received event " + state + ":" + type + ":" + path);
						for (Map.Entry<UUID,ClusterStateCallback> e: callbacks.entrySet())
						{
							ClusterStateCallback fn = e.getValue();
							fn.execute(type,path);
						}
					}

				}
			}
		};
		zk = null;
		try {
			zk = mkZk(watcher);
		}
		catch (Exception e)
		{
			LOG.error(e.getMessage(),e);
		}
	}
	private CuratorFramework mkZk() throws IOException
	{
		return zkobj.mkClient(conf,(List<String>)conf.get(Config.LEAF_ZOOKEEPER_SERVERS),
				conf.get(Config.LEAF_ZOOKEEPER_PORT),
				"");
	}


	private CuratorFramework mkZk(WatcherCallBack watcher)throws NumberFormatException,IOException
	{
		return zkobj.mkClient(conf,(List<String>)conf.get(Config.LEAF_ZOOKEEPER_SERVERS),
				conf.get(Config.LEAF_ZOOKEEPER_PORT),
				String.valueOf(conf.get(Config.LEAF_ZOOKEEPER_ROOT)),watcher);
	}

	@Override
	public void set_ephemeral_node(String path,byte[] data) throws Exception
	{
		zkobj.mkdirs(zk,PathUtils.parent_path(path));
		if (zkobj.exists(zk,path,false))
		{
			zkobj.setData(zk,path,data);
		}
		else
		{
			zkobj.createNode(zk,path,data,CreateMode.EPHEMERAL);
		}
		if (zkCache != null)
			zkCache.put(path,data);
	}

	@Override
	public void delete_node(String path) throws Exception
	{
		if (zkCache != null)
		{
			zkCache.remove(path);
		}
		zkobj.deletereRcursive(zk,path);

	}

	@Override
	public void set_data(String path,byte[] data) throws Exception
	{
		if (data.length > Utils.SIZE_1_K * 800)
		{
			throw new Exception("Writing 800k+ data into ZK is not allowed!, data size is " + data.length);
		}
		if (zkobj.exists(zk,path,false))
		{
			zkobj.setData(zk,path,data);
		}
		else
		{
			zkobj.mkdirs(zk, PathUtils.parent_path(path));
			zkobj.createNode(zk,path,data,CreateMode.PERSISTENT);
		}
		if (zkCache != null)
			zkCache.put(path,data);
	}

	@Override
	public byte[] get_data(String path,boolean watch ) throws Exception
	{
		byte[] ret = null;
		if (watch == false && zkCache != null)
		{
			ret = (byte[]) zkCache.get(path);
		}
		if (ret != null)
		{
			return ret;
		}
		ret = zkobj.getData(zk,path,watch);
		if (zkCache != null)
		{
			zkCache.put(path,ret);
		}
		return ret;
	}

	@Override
	public byte[] get_data_sync(String path, boolean watch) throws Exception
	{
		byte[] ret = null;
		ret = zkobj.getData(zk,path,watch);
		if (zkCache != null && ret != null)
			zkCache.put(path,ret);
		return ret;
	}

	@Override
	public List<String> get_childern(String path, boolean watch) throws Exception
	{
		return zkobj.getChildren(zk,path,watch);
	}

	@Override
	public void mkdirs(String path) throws Exception
	{
		zkobj.mkdirs(zk,path);
	}

	@Override
	public void tryToBeLeader(String path, byte[] host) throws Exception
	{
		zkobj.createNode(zk,path,host,CreateMode.EPHEMERAL);
	}

	@Override
	public void close()
	{
		this.active.set(false);
		zk.close();
	}

	@Override
	public boolean node_existed(String path, boolean watch) throws Exception
	{
		return zkobj.existsNode(zk,path,watch);

	}

	@Override
	public UUID register(ClusterStateCallback callback) {
		UUID id = UUID.randomUUID();
		this.callbacks.put(id,callback);
		return id;
	}

	@Override
	public String set_sequentail_node_data(String path,byte[] data) throws Exception
	{
		if (data.length > Utils.SIZE_1_K * 800)
		{
			throw new Exception("Writing 800k+ data into ZK is not allowed!, data size is " + data.length);
		}
		String sequentailPath =  zkobj.createSequentailNode(zk,path,data);

		if (zkCache != null)
			zkCache.put(sequentailPath,data);

		return sequentailPath;

	}

	@Override
	public ClusterStateCallback unregister(UUID id) {
		return this.callbacks.remove(id);
	}
	public LeafCache getZkCache() { return zkCache;}
	public void setZkCache(LeafCache zkCache) {this.zkCache = zkCache;}


}
