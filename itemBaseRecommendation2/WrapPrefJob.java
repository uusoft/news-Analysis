package itemBaseRecommendation2;

import lib.JobUtil;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WrapPrefJob extends ToolJob {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("wrong args");
			System.exit(0);
		}
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		Job job = JobUtil.prepareJob("WrapPrefJob", conf, WrapPrefMapper.class,
				Text.class, CoAndPref.class, WrapPrefReducer.class,
				Text.class, CoAndPref.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		return 1;
	}

}
