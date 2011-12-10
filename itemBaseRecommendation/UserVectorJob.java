package itemBaseRecommendation;

import lib.ListWritable;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class UserVectorJob extends ToolJob {


	
	@Override
	public int run(String[] args) throws Exception {
		
		String input = args[0];
		String output = args[1];
		
		Configuration conf = getConf();
		
		Job job = new Job(conf,"user verctor");
		job.setMapperClass(UserVectorMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(UserVectorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ListWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		return 1;
	}


}
