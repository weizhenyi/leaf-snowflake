package Config;

import java.security.InvalidParameterException;
import java.util.Map;
import utils.Utils;

/**
 * Created by weizhenyi on 2017/6/28.
 */
public class config_value {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args == null || args.length == 0) {
			throw new InvalidParameterException("Should input key name");
		}

		String key = args[0];

		Map conf = Utils.readLeafConfig();

		System.out.print("VALUE: " + String.valueOf(conf.get(key)));
	}

}
