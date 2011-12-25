package lib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtil {

	public static void delete (Path path, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(path, true);
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
