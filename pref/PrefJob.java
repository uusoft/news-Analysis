package pref;

import java.io.IOException;

import lib.FileUtil;
import lib.JobUtil;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

public class PrefJob extends ToolJob{

	public static void main(String[] args) throws Exception {

		
		PrefJob prefJob = new PrefJob();
		GenericOptionsParser parser = new GenericOptionsParser(prefJob.getConf(), args);
		System.out.println("mapred.reduce.tasks:"+prefJob.getConf().get("mapred.reduce.tasks"));
//		ToolRunner.run(prefJob, args);
		

	}

	public void runStep1(String input, String output) throws Exception {

		Job job = JobUtil.prepareJob("step1", getConf(), PrefMapper.class,
				Text.class, Text.class, PrefReducer.class, Text.class,
				Text.class);
		job.setJarByClass(PrefJob.class);
		System.out.println("num:"+job.getNumReduceTasks());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

	}

	public void runStep2(String input, String output) throws Exception {

		Job job = JobUtil.prepareJob("step1", getConf(), PrefMapper2.class,
				Text.class, Text.class, PrefReducer2.class, Text.class,
				Text.class);
		job.setJarByClass(PrefJob.class);
//		job.setNumReduceTasks(20);
		System.out.println("num:"+job.getNumReduceTasks());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

	}

	@Override
	public int run(String[] args) throws Exception {
		String step = args[0];
		String input = args[1];
		String output = args[2];
		
		if (step.equals("1")) {
			runStep1(input,output);
		}
		else if (step.equals("2")) {
			runStep2(input,output);
		}
		else if (step.equals("all")) {
			String tempPath = "tempPath";
			FileUtil.delete(new Path(tempPath), getConf());
			runStep1(input,tempPath);
			
			runStep2(tempPath,output);
		}
		return 0;
	}

}
