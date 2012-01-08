package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;



public class ConvertLog {

	public static void main (String[] args) {
//		String input = args[0];
		String input = "/usr/local/testHadoop/tempClick2.seq";
		Configuration conf = new Configuration();
		
		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(input), conf);
			Text key = new Text();
			Text val = new Text();
			while (reader.next(key, val)) {
				System.out.println(key+","+val);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
