package zookeeper;

/**
 * Created by weizhenyi on 2017/6/25.
 */
import Config.Config;
import java.io.UnsupportedEncodingException;
import java.util.Map;
public class ZookeeperAuthInfo {
	public String scheme;
	public byte[] payload = null;

	public ZookeeperAuthInfo(Map conf) {


	}

	public ZookeeperAuthInfo(String scheme, byte[] payload) {
		this.scheme = scheme;
		this.payload = payload;
	}
}