package rpc;

/**
 * Created by weizhenyi on 2017/6/27.
 */
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;


public class rpcClient {

	private static final Logger LOG = LoggerFactory.getLogger(rpcClient.class);

	public static Map<String,Long> getPeersTimeStamps(List<String> peers)
	{
		Map<String,Long> peersTimestamps = new HashMap<>();
		for(String peer : peers)
		{
			try {
				String ip = peer.split(":")[0];
				int  port = Integer.valueOf(peer.split(":")[1]);
				long timestamp = getTimestamp(ip,port,3000);
				peersTimestamps.put(peer,timestamp);
			} catch (Exception e)
			{
				LOG.warn("failed to get timestamp from peer: " + peer,e);
				continue;
			}

		}
		return peersTimestamps;
	}

	public static long getTimestamp(String ip, int port ,int timeout) throws Exception
	{
		TTransport transport = new TSocket(ip,port,timeout);
		TProtocol protocol = new TBinaryProtocol(transport);
		leafrpc.Client client = new leafrpc.Client(protocol);
		transport.open();
		long timestamp = client.gettimestamp(Utils.currentTimeMs());
		transport.close();
		return timestamp;
	}

	//for test
	public static void startClient(String ip ,int port ,int timeout) throws Exception
	{
		TTransport transport = new TSocket(ip,port,timeout);
		TProtocol protocol = new TBinaryProtocol(transport);
		leafrpc.Client client = new leafrpc.Client(protocol);
		transport.open();

		long now = Utils.currentTimeMs();
		for(int i = 0; i< 100000; i++)
		{
			System.out.println(client.gettimestamp(0L));
			//client.gettimestamp(10000000L);
		}
		System.out.println(Utils.currentTimeMs() - now);
		transport.close();
	}
	public static void main(String[] args) throws Exception
	{
		startClient("124.250.36.160",2182,10000);
	}
}
