package itemBaseRecommendation;

import java.io.IOException;

import lib.ItemCooccurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Driver {

	public static void main (String[] args) {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.out.println("please input 2 parameters: input and output");
			System.exit(0);
		}
		String input = args[0];
		String output = args[1];
		
		//userVector
//		try {
//			Job job = new Job(conf,"itemBaseRecommendation");
//			
//			job.setJarByClass(Driver.class);
//			job.setNumReduceTasks(1);
//			job.setMapperClass(userVectorMapper.class);
//			job.setReducerClass(userVectorReducer.class);
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(Text.class);
//			job.setInputFormatClass(SequenceFileInputFormat.class);
//			job.setOutputFormatClass(SequenceFileOutputFormat.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(ArrayWritable.class);
//			
//			FileInputFormat.addInputPath(job, new Path(input));
//			FileOutputFormat.setOutputPath(job, new Path(output));
//			
//			job.waitForCompletion(true);
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		//cooccurrence
		try {
			Job job = new Job(conf,"itemBaseRecommendation");
			
			job.setJarByClass(Driver.class);
			job.setNumReduceTasks(1);
			job.setMapperClass(cooccurrenceMapper.class);
			job.setReducerClass(cooccurrenceReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(ItemCooccurrence.class);
			
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
