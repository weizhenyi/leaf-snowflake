package utils;

/**
 * Created by weizhenyi on 2017/6/25.
 */


import Config.Config;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PathUtils {
	static Logger LOG = LoggerFactory.getLogger(PathUtils.class);

	public static final String SEPERATOR = "/";

	/**
	 * split path as list
	 *
	 * @param path
	 * @return
	 */
	public static List<String> tokenize_path(String path) {
		String[] toks = path.split(SEPERATOR);
		ArrayList<String> rtn = new ArrayList<String>();
		for (String str : toks) {
			if (!str.isEmpty()) {
				rtn.add(str);
			}
		}
		return rtn;
	}

	public static String toks_to_path(List<String> toks) {
		StringBuffer buff = new StringBuffer();
		buff.append(SEPERATOR);
		int size = toks.size();
		for (int i = 0; i < size; i++) {
			buff.append(toks.get(i));
			if (i < (size - 1)) {
				buff.append(SEPERATOR);
			}

		}
		return buff.toString();
	}

	public static String normalize_path(String path) {
		String rtn = toks_to_path(tokenize_path(path));
		return rtn;
	}

	public static String parent_path(String path) {
		List<String> toks = tokenize_path(path);
		int size = toks.size();
		if (size > 0) {
			toks.remove(size - 1);
		}
		return toks_to_path(toks);
	}

	public static String full_path(String parent, String name) {
		return normalize_path(parent + SEPERATOR + name);
	}

	public static boolean exists_file(String path) {
		return (new File(path)).exists();
	}

	public static void rmr(String path) throws IOException {
		LOG.debug("Rmr path " + path);
		if (exists_file(path)) {
			FileUtils.forceDelete(new File(path));
		}

	}

	public static void local_mkdirs(String path) throws IOException {
		LOG.debug("Making dirs at" + path);
		FileUtils.forceMkdir(new File(path));
	}

	public static void rmpath(String path) {
		LOG.debug("Removing path " + path);
		boolean isdelete = (new File(path)).delete();
		if (!isdelete) {
			throw new RuntimeException("Failed to delete " + path);
		}
	}

	public static void touch(String path) throws IOException {
		LOG.debug("Touching file at" + path);
		boolean success = (new File(path)).createNewFile();
		if (!success) {
			throw new RuntimeException("Failed to touch " + path);
		}
	}

	public static List<String> read_dir_contents(String dir) {
		ArrayList<String> rtn = new ArrayList<String>();
		if (exists_file(dir)) {
			File[] list = (new File(dir)).listFiles();
			for (File f : list) {
				rtn.add(f.getName());
			}
		}
		return rtn;
	}

	public static String getCanonicalPath(String fileName) {
		String ret = null;
		File file = new File(fileName);
		if (file.exists()) {
			try {
				ret = file.getCanonicalPath();
			} catch (IOException e) {
				LOG.error("", e);
			}
		} else {
			LOG.warn(fileName + " doesn't exist ");
		}

		return ret;
	}

	public static String leafLocalDir(Map conf) throws IOException
	{
		String ret = String.valueOf(conf.get(Config.LEAF_LOCAL_DIR));
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e)
		{
			LOG.error("Failed to create dir " + ret ,e);
			throw e;
		}
		return ret;
	}
	public static String leafPids(Map conf) throws IOException
	{
		String ret = leafLocalDir(conf) + Config.FILE_SEPERATEOR + "pids";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e)
		{
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}




}
