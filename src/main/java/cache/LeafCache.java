package cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import Config.Config;
/**
 * Created by weizhenyi on 2017/6/25.
 */
public interface LeafCache extends Serializable{ //缓存 一些 存储到 zk上面的 数据，key是 zk上的path，value是 zk节点上的值

	public static final String TAG_TIMEOUT_LIST = Config.CACHE_TIMEOUT_LIST;

	void init(Map<Object, Object> conf) throws Exception;

	void cleanup();

	Object get(String key);

	void getBatch(Map<String, Object> map);

	void remove(String key);

	void removeBatch(Collection<String> keys);

	void put(String key, Object value, int timeoutSecond);

	void put(String key, Object value);

	void putBatch(Map<String, Object> map);

	void putBatch(Map<String, Object> map, int timeoutSeconds);

}
