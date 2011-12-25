package itemBaseRecommendation2;


import java.io.IOException;

import lib.FileUtil;
import lib.ItemCooccurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class Driver {

	public static void main (String[] args) {
		String step = args[0];
		String[] newArgs = new String[args.length-1];
		for (int i=1; i<args.length; i++) {
			newArgs[i-1] = args[i];
		}
		Configuration conf = new Configuration();
		try {
			if ("1".equals(step)) {
				ToolRunner.run(new PrefJob(), newArgs);
			}
			else if ("2".equals(step)) {
				ToolRunner.run(new CooccurrenceJob(), newArgs);
			}
			else if ("3".equals(step)) {
				ToolRunner.run(new WrapPrefJob(), newArgs);
			}
			else if ("all".equals(step)) {
				String input = newArgs[0];
				String output = "InternOutput1";
				FileUtil.delete(new Path(output), conf);
				ToolRunner.run(new PrefJob(), newArgs);
				input = output;
				output = "InternOutput2";
				newArgs[0] = input;
				newArgs[1] = output;
				FileUtil.delete(new Path(output), conf);
				ToolRunner.run(new CooccurrenceJob(), newArgs);
				input = "InternOutput1";
				output = "InternOutput3";
				newArgs[0] = input;
				newArgs[1] = output;
				FileUtil.delete(new Path(output), conf);
				ToolRunner.run(new WrapPrefJob(), newArgs);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		
	}
}
