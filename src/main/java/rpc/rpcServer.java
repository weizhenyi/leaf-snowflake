package rpc;

/**
 * Created by weizhenyi on 2017/6/27.
 */
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import leaf.leafServer;

public class rpcServer {
	private static final Logger LOG = LoggerFactory.getLogger(rpcServer.class);

	//TThreadPoolServer
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
		LOG.info("leaf RPCServer(type:TThreadPoolServer) start at ip:port : "+ ip +":" + port );
		server.serve();
	}
	//TThreadedSelectorServer
	public static void startRPCServer2(leafServer leafserver , String ip , int port) throws Exception
	{
		//关联处理器leafrpc的实现
		TProcessor processor = new leafrpc.Processor<leafrpc.Iface>(new RPCService(leafserver));
		//传输通道，非阻塞模式
		InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(ip),port);
		TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(address,10000);
		//多线程半同步半异步
		TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
		tArgs.processor(processor);
		//二进制协议
		tArgs.protocolFactory(new TBinaryProtocol.Factory());
		//多线程半同步半异步的服务模型
		TServer server = new TThreadedSelectorServer(tArgs);
		LOG.info("leaf RPCServer(type:TThreadedSelectorServer) start at ip:port : "+ ip +":" + port );
		server.serve();
	}
	//TNonblockingServerSocket
	public static void startRPCServer3(leafServer leafserver , String ip , int port) throws Exception
	{
		TProcessor processor = new leafrpc.Processor<leafrpc.Iface>(new RPCService(leafserver));
		//传输通道，非阻塞模式
		InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(ip),port);
		TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(address,10000);
		TNonblockingServer.Args args = new TNonblockingServer.Args(serverTransport);
		args.protocolFactory(new TBinaryProtocol.Factory());
		args.transportFactory(new TFramedTransport.Factory());
		args.processorFactory(new TProcessorFactory(processor));
		TServer server = new TNonblockingServer(args);
		LOG.info("leaf RPCServer(type:TNonblockingServerSocket) start at ip:port : "+ ip +":" + port );
		server.serve();
	}

	public static void startRPCServer4(leafServer leafserver , String ip , int port) throws Exception
	{
		TProcessor processor = new leafrpc.Processor<leafrpc.Iface>(new RPCService(leafserver));
		//传输通道，非阻塞模式
		InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(ip),port);
		TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(address,10000);
		THsHaServer.Args  args = new THsHaServer.Args(serverTransport);
		args.processor(processor);
		args.protocolFactory(new TBinaryProtocol.Factory());
		args.transportFactory(new TFramedTransport.Factory());
		TServer server = new THsHaServer(args);
		LOG.info("leaf RPCServer(type:THsHaServer) start at ip:port : "+ ip +":" + port );
		server.serve();
	}

	public static void main(String[] args) throws Exception
	{

	}
}
