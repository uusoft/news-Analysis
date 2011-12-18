package itemBaseRecommendation2;


import java.io.IOException;

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
		try {
			if ("1".equals(step)) {
				ToolRunner.run(new UserVectorJob(), newArgs);
			}
			else if ("2".equals(step)) {
				ToolRunner.run(new CooccurrenceJob(), newArgs);
			}
			else if ("3".equals(step)) {
				ToolRunner.run(new WrapPrefJob(), newArgs);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		Configuration conf = new Configuration();
//		if (args.length != 2) {
//			System.out.println("please input 2 parameters: input and output");
//			System.exit(0);
//		}
//		String input = args[0];
//		String output = args[1];
		
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
//		try {
//			Job job = new Job(conf,"itemBaseRecommendation");
//			
//			job.setJarByClass(Driver.class);
//			job.setNumReduceTasks(1);
//			job.setMapperClass(cooccurrenceMapper.class);
//			job.setReducerClass(cooccurrenceReducer.class);
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(Text.class);
//			job.setInputFormatClass(SequenceFileInputFormat.class);
//			job.setOutputFormatClass(SequenceFileOutputFormat.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(ItemCooccurrence.class);
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
		
	}
}
