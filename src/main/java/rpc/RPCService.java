package rpc;

import org.apache.thrift.TException;
import utils.Utils;
import leaf.leafServer;

/**
 * Created by weizhenyi on 2017/6/27.
 */
public class RPCService implements leafrpc.Iface{
	private leafServer server = null;
	public RPCService(leafServer server)
	{
		this.server = server;
	}


	@Override
	public long gettimestamp(long mytimestamp) throws TException {
		return Utils.currentTimeMs();
	}

	@Override
	public String getID(String para) throws TException {
		return server.makeFinalId();
	}
}
