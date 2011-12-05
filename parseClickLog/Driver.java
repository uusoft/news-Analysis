package parseClickLog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("parameters are not available");
			System.exit(0);
		}
		String inputPath = args[0];
		String outputPath = args[1];
		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf, "parse click log");

			job.setNumReduceTasks(10);
			job.setJarByClass(Driver.class);
			job.setMapperClass(ParseMapper.class);
			job.setReducerClass(ParseReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(Text.class);
//			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

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
