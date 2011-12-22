package parse.lastvisit;

import java.io.IOException;

import lib.JobUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
			Job job = JobUtil.prepareJob("get lastVisit", conf, LastVisitMapper.class,
					Text.class, Text.class, LastVisitReducer.class, Text.class,
					Text.class);

			job.setJarByClass(Driver.class);
			job.setNumReduceTasks(10);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);

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
