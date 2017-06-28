package zookeeper;
import java.util.List;
import java.util.UUID;
import callback.ClusterStateCallback;
/**
 * Created by weizhenyi on 2017/6/25.
 */
public interface ClusterStat {
	public void set_ephemeral_node(String path,byte[] data) throws Exception;
	public void delete_node(String path) throws Exception;
	public void set_data(String path,byte[] data) throws Exception;
	public String set_sequentail_node_data(String path,byte[] data) throws Exception;
	public byte[] get_data(String path,boolean watch ) throws Exception;
	public byte[] get_data_sync(String path, boolean watch) throws Exception;
	public List<String> get_childern(String path, boolean watch) throws Exception;
	public void mkdirs(String path) throws Exception;
	public void tryToBeLeader(String path, byte[] host) throws Exception;
	public void close();
	public boolean node_existed(String path, boolean watch) throws Exception;
	public UUID register(ClusterStateCallback callback);
	public ClusterStateCallback unregister(UUID id);

}
