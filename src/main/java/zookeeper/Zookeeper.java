package zookeeper;

import java.util.Map;
import java.util.List;

import callback.WatcherCallBack;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import utils.Utils;
import utils.PathUtils;
import callback.DefaultWatcherCallBack;
/**
 * Created by weizhenyi on 2017/6/25.
 */
public class Zookeeper {
	private static  Logger LOG = LoggerFactory.getLogger(Zookeeper.class);
	public CuratorFramework mkClient(Map conf, List<String> servers , Object port, String root)
	{
		return mkClient(conf,servers,port,root,new DefaultWatcherCallBack());
	}

	/**
	 *  connect to ZK, register Watcher & unhandled Watcher
	 */
	public CuratorFramework mkClient(Map conf, List<String> servers , Object port , String root , final WatcherCallBack watcher)
	{
		CuratorFramework fk = Utils.newCurator(conf,servers,port,root);
		fk.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework _fk, CuratorEvent e) throws Exception {
				if (e.getType().equals(CuratorEventType.WATCHED)) //WATCHED事件对应着CuratorFrame的watched()方法
				{
					WatchedEvent event = e.getWatchedEvent();//收到了zk事件
					if (event != null)
					{
						LOG.info("get event: " + event + " eventType: " + event.getType() + " eventPath: "+event.getPath() );
						watcher.execute(event.getState(),event.getType(),event.getPath());
					}
				}
				if (e.getType().equals(CuratorEventType.CHILDREN)) //对应着getChildren()方法
				{
					WatchedEvent event = e.getWatchedEvent();//收到了zk事件
					if (event != null)
					{
						LOG.info("get event: " + event + " eventType: " + event.getType() + " eventPath: "+event.getPath()  + "----e.getType().equals(CuratorEventType.CHILDREN" );
						watcher.execute(event.getState(),event.getType(),event.getPath());
					}
				}
				if (e.getType().equals(CuratorEventType.CLOSING)) //对应着 close（）方法
				{
					WatchedEvent event = e.getWatchedEvent();//收到了zk事件
					if (event != null)
					{
						LOG.info("get event: " + event + " eventType: " + event.getType() + " eventPath: "+event.getPath()  + "----e.getType().equals(CuratorEventType.CLOSING" );
						watcher.execute(event.getState(),event.getType(),event.getPath());
					}
				}
				if(e.getType().equals(CuratorEventType.GET_DATA))
				{
					WatchedEvent event = e.getWatchedEvent();//收到了zk事件
					if (event != null)
					{
						LOG.info("get event: " + event + " eventType: " + event.getType() + " eventPath: "+event.getPath()  + "----e.getType().equals(CuratorEventType.GET_DATA)" );
						watcher.execute(event.getState(),event.getType(),event.getPath());
					}
				}
				if(e.getType().equals(CuratorEventType.SET_DATA))
				{
					WatchedEvent event = e.getWatchedEvent();//收到了zk事件
					if (event != null)
					{
						LOG.info("get event: " + event + " eventType: " + event.getType() + " eventPath: "+event.getPath()  + "----e.getType().equals(CuratorEventType.SET_DATA)" );
						watcher.execute(event.getState(),event.getType(),event.getPath());
					}
				}
			}
		});
		fk.getUnhandledErrorListenable().addListener(new UnhandledErrorListener()
		{
			@Override
			public void unhandledError(String s, Throwable throwable) {
				String errmsg = "Unrecoverable Zookeeper error,halting process:" + s;
				LOG.error(errmsg,throwable);
				Utils.halt_process(1,"Unrecoverable Zookeeper error!");
			}
		});
		fk.start();
		return fk;
	}
	public boolean existsNode(CuratorFramework zk , String path, boolean watch) throws Exception
	{
		Stat stat = null;
		if (watch)
		{
			stat = zk.checkExists().watched().forPath(PathUtils.normalize_path(path));
		}
		else
		{
			stat = zk.checkExists().forPath(PathUtils.normalize_path(path));
		}
		return stat != null;
	}

	public String createNode(CuratorFramework zk, String path, byte[] data , CreateMode mode) throws Exception
	{
		String npath = PathUtils.normalize_path(path);
		return zk.create().withMode(mode).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(npath,data);
	}

	public String createSequentailNode(CuratorFramework zk, String path, byte[] data ) throws Exception
	{
		String npath = PathUtils.normalize_path(path);
		String parent = PathUtils.parent_path(path);
		if ( !existsNode(zk,parent,false) )
		{
			createNode(zk,parent,"".getBytes());
		}
		return zk.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(npath,data);
	}

	public String createNode(CuratorFramework zk , String path , byte[] data) throws Exception
	{
		return createNode(zk,path,data,CreateMode.PERSISTENT);
	}

	public void mkdirs(CuratorFramework zk , String path) throws Exception
	{
		String npath = PathUtils.normalize_path(path);
		// the node is "/"
		if (npath.equals("/"))
		{
			return;
		}
		// the node exist
		if (existsNode(zk,path,false)) //如果zk上面已经有该目录了
		{
			return;
		}
		mkdirs(zk,PathUtils.parent_path(npath));//递归创建父目录
		try
		{
			createNode(zk,path,Utils.barr((byte)7),CreateMode.PERSISTENT);
		}
		catch (KeeperException e)
		{
			LOG.warn("zookeepr mkdir faild  for " + path  );
		}

	}
	public void deleteNode(CuratorFramework zk , String path) throws Exception
	{
		zk.delete().forPath(PathUtils.normalize_path(path));
	}

	public byte[] getData(CuratorFramework zk, String path , boolean watch) throws Exception
	{
		String npath = PathUtils.normalize_path(path);
		try
		{
			if (existsNode(zk,path,watch))
			{
				if (watch)
				{
					return zk.getData().watched().forPath(npath);
				}
				else
				{
					return zk.getData().forPath(npath);
				}
			}
		}
		catch (KeeperException e)
		{
			LOG.error("zookeeper getdata for path:" + path ,e);
		}
		return null;
	}

	public List<String> getChildren(CuratorFramework zk, String path, boolean watch) throws Exception
	{
		String npath = PathUtils.normalize_path(path);
		if (watch)
		{
			return zk.getChildren().watched().forPath(npath);
		}
		else
		{
			return zk.getChildren().forPath(npath);
		}
	}

	public Stat setData(CuratorFramework zk, String path , byte[] data) throws Exception
	{
		String npath = PathUtils.normalize_path(path);
		return zk.setData().forPath(npath,data);

	}

	public boolean exists(CuratorFramework zk, String path , boolean watch) throws Exception
	{
		return existsNode(zk,path,watch);
	}

	public void deletereRcursive(CuratorFramework zk , String path) throws Exception
	{
		String npath = PathUtils.normalize_path(path);
		if (existsNode(zk,npath,false))
		{
			zk.delete().guaranteed().deletingChildrenIfNeeded().forPath(npath);
		}
	}




}
