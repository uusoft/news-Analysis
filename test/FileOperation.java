package test;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileOperation {

	public static void read(String filename) {
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			InputStream in = fs.open(new Path(filename));
			IOUtils.copyBytes(in, System.out, 4096, false);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main (String[] args) {
		read(args[0]);
	}
}
