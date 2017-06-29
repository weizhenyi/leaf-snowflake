/**
 * Created by weizhenyi on 2017/6/25.
 */
import leaf.leafServer;
import org.apache.commons.io.FileUtils;
import utils.PathUtils;
import utils.Utils;
import zookeeper.DistributedClusterStat;
import java.util.*;
import Config.Config;


public class test {
	public static void main(String[] args) throws Exception
	{
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("hello");
			}
		}));
		for(int i = 0; i < 1000; i++)
		{
			if (i == 345)
			{
				//Utils.halt_process(-1,"hi");
				try
				{
					//System.exit(-1);
					Utils.halt_process(-1,"hello world");
				}
				catch (Exception e)
				{
				}
				finally {
					System.out.println("hello2");
				}
			}
		}
	}
}
