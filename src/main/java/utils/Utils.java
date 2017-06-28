package utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.File;
import java.net.NetworkInterface;
import java.util.concurrent.TimeUnit;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import org.jboss.netty.util.internal.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.json.simple.JSONValue;
import zookeeper.ZookeeperAuthInfo;
import Config.Config;
/**
 * Created by weizhenyi on 2017/6/25.
 */
public class Utils {
	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	public static long SIZE_1_K = 1024;
	public static long SIZE_1_M = SIZE_1_K * 1024;
	public static long SIZE_1_G = SIZE_1_M * 1024;
	public static long SIZE_1_T = SIZE_1_G * 1024;
	public static long SIZE_1_P = SIZE_1_T * 1024;

	public static final int MIN_1 = 60;
	public static final int MIN_30 = MIN_1 * 30;
	public static final int HOUR_1 = MIN_30 * 2;
	public static final int DAY_1 = HOUR_1 * 24;//60*30*2*24










	public static CuratorFramework newCurator(Map conf , List<String> servers ,Object port, String root)
	{
		return newCurator(conf,servers,port,root,null);
	}

	public static CuratorFramework newCurator(Map conf , List<String> servers , Object port, String root, ZookeeperAuthInfo info)
	{
		List<String> serverPorts = new ArrayList<>();
		for(String zkServer : servers)
		{
			serverPorts.add(zkServer + ":" + Utils.getInt(port));
		}
		String zkStr = StringUtils.join(serverPorts,',') + PathUtils.normalize_path(root);
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

		setupBuilder(builder,zkStr,conf,info);

		return builder.build();
	}

	protected static void setupBuilder(CuratorFrameworkFactory.Builder builder , String zkStr, Map conf, ZookeeperAuthInfo auth)
	{
		builder.connectString(zkStr)
				.connectionTimeoutMs(Utils.getInt(conf.get(Config.LEAF_ZOOKEEPER_CONNECTION_TIMEOUT)))
				.sessionTimeoutMs(Utils.getInt(conf.get(Config.LEAF_ZOOKEEPER_SESSION_TIMEOUT)))
				.retryPolicy(new LeafBoundedExponentialBackoffRetry(Utils.getInt(conf.get(Config.LEAF_ZOOKEEPER_RETRY_INTERVAL)),
						Utils.getInt(conf.get(Config.LEAF_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
						Utils.getInt(conf.get(Config.LEAF_ZOOKEEPER_RETRY_TIMES))));
		if (auth != null && auth.scheme != null && auth.payload != null)
		{
			builder = builder.authorization(auth.scheme,auth.payload);
		}


	}

	public static int getInt(Object obj)
	{
		return getInt(obj,2181);
	}
	public static int getInt(Object obj , int defaultValue )
	{
		if ( obj == null )
			return defaultValue;
		if ( obj instanceof Number)
			return  ((Number)obj).intValue();
		else if (obj instanceof String)
			try
			{
				return Integer.parseInt((String)obj);
			}
			catch (NumberFormatException e)
			{
				throw  e;
			}
		else
			throw new IllegalArgumentException(" can not convert " + obj + " to int!");
	}

	public static void haltProcess(int val) {
		 Runtime.getRuntime().halt(val);
	}

	public static void halt_process(int val, String msg) {
		LOG.info("Halting process: " + msg);
		if(true)
			return;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		haltProcess(val);

	}

	public static byte[] barr(byte v) {
		byte[] byteArray = new byte[1];
		byteArray[0] = v;

		return byteArray;
	}

	public static Map readDefaultConfig()
	{
		return LoadConf.findAndReadYaml("defaults.yaml",true,false);
	}

	public static Map loadDefinedConf(String confFile)
	{
		File file = new File(confFile);
		if ( !file.exists())
		{
			return LoadConf.findAndReadYaml(confFile,true,false);
		}
		Yaml yaml = new Yaml();
		Map ret;
		try {
			ret = (Map) yaml.load(new FileReader(file));
		}
		catch (FileNotFoundException e)
		{
			ret = null;
		}
		if (ret == null)
			ret = new HashMap();
		return ret;

	}

	public static Map readCommandLineOpts() {
		Map ret = new HashMap();
		String commandOptions = System.getProperty("leaf.options");
		if (commandOptions != null) {
			String[] configs = commandOptions.split(",");
			for (String config : configs) {
				config = URLDecoder.decode(config);
				String[] options = config.split("=", 2);
				if (options.length == 2) {
					Object val = JSONValue.parse(options[1]);
					if (val == null) {
						val = options[1];
					}
					ret.put(options[0], val);
				}
			}
		}
		return ret;
	}

	public static void replaceLocalDir(Map<Object, Object> conf) {
		String leafHome = System.getProperty("leaf.home");
		boolean isEmpty = StringUtils.isBlank(leafHome);

		Map<Object, Object> replaceMap = new HashMap<Object, Object>();

		for (Map.Entry entry : conf.entrySet()) {
			Object key = entry.getKey();
			Object value = entry.getValue();

			if (value instanceof String) {
				if (StringUtils.isBlank((String) value) == true) {
					continue;
				}

				String str = (String) value;
				if (isEmpty == true) {
					str = str.replace("%LEAF_HOME%", ".");
				} else {
					str = str.replace("%LEAF_HOME%", leafHome);
				}

				replaceMap.put(key, str);
			}
		}

		conf.putAll(replaceMap);
	}

	public static Map readLeafConfig()
	{
		Map ret = readDefaultConfig();
		String confFile = System.getProperty("leaf.conf.file");
		Map leaf ;
		if (StringUtils.isBlank(confFile))
		{
			leaf = LoadConf.findAndReadYaml("leaf.yaml",false,false);
		}
		else
		{
			leaf = loadDefinedConf(confFile);
		}
		ret.putAll(leaf);
		ret.putAll(readCommandLineOpts());
		replaceLocalDir(ret);
		return ret;
	}

	/**
	 * Gets the pid of this JVM, because Java doesn't provide a real way to do this.
	 *
	 * @return
	 */
	private static String process_pid() {
		String name = ManagementFactory.getRuntimeMXBean().getName();
		String[] split = name.split("@");
		if (split.length != 2) {
			throw new RuntimeException("Got unexpected process name: " + name);
		}

		return split[0];
	}

	public static String createPid( String pidDir) throws Exception
	{
		if ( !PathUtils.exists_file(pidDir) )
		{
			FileUtils.forceMkdir(new File(pidDir));
		}
		if (! new File(pidDir).isDirectory() )
		{
			throw new RuntimeException("pid dir: " + pidDir + "is not directory.");
		}
		List<String> existPids = PathUtils.read_dir_contents(pidDir);
		//获得当前进程ID
		String pid = process_pid();
		String pidPath = pidDir + Config.FILE_SEPERATEOR + pid;
		PathUtils.touch(pidPath);
		LOG.info("Successfully touch pid: " + pidPath );

		for (String existPid : existPids)
		{
			try
			{
				kill(Integer.valueOf(existPid));
				PathUtils.rmpath(pidDir + Config.FILE_SEPERATEOR + existPid);
			} catch (Exception e)
			{
				LOG.warn(e.getMessage(),e);
			}
		}
		return pidPath;

	}
	public static String createPid( Map conf) throws Exception
	{
		String pidDir = PathUtils.leafPids(conf);
		return createPid(pidDir);
	}

	public static void kill(Integer pid) {
		process_killed(pid);

		sleepMs(5 * 1000);

		ensure_process_killed(pid);
	}

	public static void exec_command(String command) throws ExecuteException, IOException {
		String[] cmdlist = command.split(" ");
		CommandLine cmd = new CommandLine(cmdlist[0]);
		for (int i = 1; i < cmdlist.length; i++) {
			cmd.addArgument(cmdlist[i]);
		}

		DefaultExecutor exec = new DefaultExecutor();
		exec.execute(cmd);
	}

	public static void process_killed(Integer pid) {
		try {
			exec_command("kill " + pid);
			LOG.info("kill process " + pid);
		} catch (ExecuteException e) {
			LOG.info("Error when trying to kill " + pid + ". Process has been killed. ");
		} catch (Exception e) {
			LOG.info("Error when trying to kill " + pid + ".Exception ", e);
		}
	}

	public static void ensure_process_killed(Integer pid) {
		// in this function, just kill the process 5 times
		// make sure the process be killed definitely
		for (int i = 0; i < 5; i++) {
			try {
				exec_command("kill -9 " + pid);
				LOG.info("kill -9 process " + pid);
				sleepMs(100);
			} catch (ExecuteException e) {
				LOG.info("Error when trying to kill " + pid + ". Process has been killed");
			} catch (Exception e) {
				LOG.info("Error when trying to kill " + pid + ".Exception ", e);
			}
		}
	}

	public static void sleepMs(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {

		}
	}

	public static byte[] barr(Integer v) {
		byte[] byteArray = new byte[Integer.SIZE / 8];
		for (int i = 0; i < byteArray.length; i++) {
			int off = (byteArray.length - 1 - i) * 8;
			byteArray[i] = (byte) ((v >> off) & 0xFF);
		}
		return byteArray;
	}



	public static int bytesToint( byte[] b) throws Exception
	{
		if (b.length != 4)
		{
			throw new RuntimeException("can not convert bytes to int for the bytes length is :" + b.length);
		}
		int b1 = ((int)b[0] & 0x000000FF) << 24;
		int b2 = ((int)b[1] & 0x000000FF) << 16;
		int b3 = ((int)b[2] & 0x000000FF) << 8;
		int b4 = ((int)b[3] & 0x000000FF) ;


		int result = b1 | b2 | b3 | b4 ;
		return result;
	}

	public static long bytesTolong(byte[] b) throws Exception
	{
		if (b.length != 8)
		{
			throw new RuntimeException("can not convert bytes to long for the bytes length is :" + b.length);
		}
		long b1 = ((long)b[0] & 0x00000000000000FF) << 56;
		long b2 = ((long)b[1] & 0x00000000000000FF) << 48;
		long b3 = ((long)b[2] & 0x00000000000000FF) << 40;
		long b4 = ((long)b[3] & 0x00000000000000FF) << 32;
		long b5 = ((long)b[4] & 0x00000000000000FF) << 24;
		long b6 = ((long)b[5] & 0x00000000000000FF) << 16;
		long b7 = ((long)b[6] & 0x00000000000000FF) << 8;
		long b8 = ((long)b[7] & 0x00000000000000FF) ;

		long result = b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8;
		return result;

	}

	public static byte[] barr(Long v)
	{
		byte[] byteArray = new byte[Long.SIZE / 8];
		for (int i = 0; i < byteArray.length; i++) {
			int off = (byteArray.length - 1 - i) * 8;
			byteArray[i] = (byte) ((v >> off) & 0xFF);
		}
		return byteArray;
	}

	public static String getHostName()
	{
		try {
			String hostname = InetAddress.getLocalHost().getHostName();
			if ( hostname.equals("localhost") || hostname.equals("localhost.localdomain")
					|| hostname.equals("localhost4") || hostname.equals("localhost4.localdomain4")
					|| hostname.equals("localhost6") || hostname.equals("localhost6.localdomain6"))
			{
				return null;
			}
			return hostname;
		} catch (Exception e)
		{
			LOG.error("Failed to get localhost name.");
			return null;
		}
	}

	public static byte[] getInetAdress() {
		final String regex = "^172.20.(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\.(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
		byte[] ip = null;
		Enumeration<NetworkInterface> netInterfaces = null;
		try {
			netInterfaces = NetworkInterface.getNetworkInterfaces();
			while (netInterfaces.hasMoreElements()) {
				NetworkInterface ni = (NetworkInterface) netInterfaces
						.nextElement();

				Enumeration<InetAddress> ips = ni.getInetAddresses();
				while (ips.hasMoreElements()) {
					InetAddress current_addr = (InetAddress) ips.nextElement();
					if (current_addr instanceof Inet4Address) {
						String address = current_addr.getHostAddress();
						if (address.matches(regex)) {
							String[] add = address.split("\\.");
							byte[] re = { Integer.valueOf(add[0]).byteValue() ,
									Integer.valueOf(add[1]).byteValue(),
									Integer.valueOf(add[2]).byteValue(),
									Integer.valueOf(add[3]).byteValue() };
							return re;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ip;
	}

	/**
	 * 获得内网IP
	 * @return 内网IP
	 */
	public static String getIntranetIp(){
		try{
			return InetAddress.getLocalHost().getHostAddress();
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	/**
	 * 获得外网IP
	 * @return 外网IP
	 */
	public static String getInternetIp(){
		if( true )
			return getIntranetIp();
		try{
			Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
			InetAddress ip = null;
			Enumeration<InetAddress> addrs;
			while (networks.hasMoreElements())
			{
				addrs = networks.nextElement().getInetAddresses();
				while (addrs.hasMoreElements())
				{
					ip = addrs.nextElement();
					if (ip != null
							&& ip instanceof Inet4Address
							&& ip.isSiteLocalAddress()
							&& !ip.getHostAddress().equals(getIntranetIp()))
					{
						return ip.getHostAddress();
					}
				}
			}

			// 如果没有外网IP，就返回内网IP
			return getIntranetIp();
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	/**
	 *  获得当前时间毫秒数
	 * */
	public static long currentTimeMs()
	{
		return System.currentTimeMillis();
	}

	public static String serverNodePathPrefix(Map conf)
	{
		return String.valueOf(conf.get(Config.LEAF_ZOOKEEPER_FOREVER)) + Config.FILE_SEPERATEOR + Utils.getIntranetIp() + ":" + String.valueOf(conf.get(Config.LEAF_RPC_PORT))+"-";
	}

	public static String serverNodePath(Map conf , String serverId)
	{
		return String.valueOf(conf.get(Config.LEAF_ZOOKEEPER_FOREVER)) + Config.FILE_SEPERATEOR  + serverId;
	}

	public static String localDataPath(Map conf)
	{
		return String.valueOf(conf.get(Config.LEAF_LOCAL_DIR)) + Config.FILE_SEPERATEOR + "data";
	}

	public static String extractServerId( String str)
	{
		return null;
	}

	public static String formatTimeStampMs (long timestamp)
	{
		String DATE_FORMAT_T = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_T);
		return sdf.format(new Date(timestamp));
	}

	public static String getIPPort(Map conf)
	{
		return getIntranetIp() + ":" + String.valueOf(conf.get(Config.LEAF_RPC_PORT));
	}

	public static String getInternetIPPort(Map conf)
	{
		return getIntranetIp() + ":" + String.valueOf(conf.get(Config.LEAF_RPC_PORT));
	}

	public static String getEphemeralNodePath(Map conf)
	{
		return String.valueOf(conf.get(Config.LEAF_ZOOKEEPER_EPHEMERAL)) + Config.FILE_SEPERATEOR + getIPPort(conf);
	}
}
