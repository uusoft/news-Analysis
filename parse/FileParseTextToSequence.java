package parse;

import java.io.IOException;

import lib.JobUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FileParseTextToSequence {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable pos, Text line, Context context) {
			String[] tmp = line.toString().split("\t");
			if (tmp.length !=2)
				return;
			String key = tmp[0];
			String value = tmp[1];

			try {
				context.write(new Text(key), new Text(value));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> valueList, Context context) {
			for (Text value : valueList) {
				try {
					context.write(key, value);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("wrong args");
			System.exit(0);
		}
		String input = args[0];
		String output = args[1];

		Configuration conf = new Configuration();
		try {
			Job job = JobUtil.prepareJob("FileParseTextToSequnce", conf,
					Mapper1.class, Text.class, Text.class,
					Reducer1.class, Text.class, Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			job.waitForCompletion(true);
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
