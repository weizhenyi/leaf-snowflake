package rpc;

/**
 * Created by weizhenyi on 2017/6/27.
 */
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.server.TThreadPoolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import java.net.InetAddress;
import java.net.ServerSocket;
import leaf.leafServer;

public class rpcServer {
	private static final Logger LOG = LoggerFactory.getLogger(rpcServer.class);

	public static void startRPCServer(leafServer leafserver , String ip , int port) throws Exception
	{
		ServerSocket serverSocket = new ServerSocket(port,10000, InetAddress.getByName(ip));

		TServerSocket serverTransport = new TServerSocket(serverSocket);

		//设置协议工厂为TBinaryProtocolFactory
		Factory proFactory = new TBinaryProtocol.Factory();
		//关联处理器leafrpc的实现
		TProcessor processor = new leafrpc.Processor<leafrpc.Iface>(new RPCService(leafserver));
		TThreadPoolServer.Args args2 = new TThreadPoolServer.Args(serverTransport);
		args2.processor(processor);
		args2.protocolFactory(proFactory);
		TServer server = new TThreadPoolServer(args2);
		LOG.info("leaf RPCServer start at ip:port : "+ ip +":" + port );
		server.serve();
	}
	public static void main(String[] args) throws Exception
	{


	}
}
