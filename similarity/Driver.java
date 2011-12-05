package similarity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Driver {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("inputPath outputPath");
			System.exit(0);
		}
		String inputPath = args[0];
		String outputPath = args[1];
		
		String outputPathStep1 = "similarityStep1Out";
		String outputPathStep2 = "similarityStep2Out";
		
		Configuration conf = new Configuration();
		try {
			//mr step1
			Job job1 = new Job(conf, "parse click log");
//
			job1.setNumReduceTasks(10);
			job1.setJarByClass(Driver.class);
			job1.setMapperClass(SimilarityMapper.class);
			job1.setReducerClass(SimilarityReducer.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job1, new Path(inputPath));
			FileOutputFormat.setOutputPath(job1, new Path(outputPathStep1));
			
			
			FileSystem fs = FileSystem.get(conf);
			
			if (fs.exists(new Path(outputPathStep1))) {
				fs.delete(new Path(outputPathStep1), true);
			}
			job1.waitForCompletion(true);

			//mr step 2 
//			Job job2 = new Job(conf, "parse click log");
//			job2 = new Job(conf, "parse click log");
//			job2.setNumReduceTasks(10);
//			job2.setJarByClass(Driver.class);
//			job2.setMapperClass(SimilarityMapper2.class);
//			job2.setReducerClass(SimilarityReducer2.class);
//			job2.setMapOutputKeyClass(Text.class);
//			job2.setMapOutputValueClass(IntWritable.class);
//			job2.setInputFormatClass(SequenceFileInputFormat.class);
//			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
//			job2.setOutputKeyClass(Text.class);
//			job2.setOutputValueClass(IntWritable.class);
//			
//			FileInputFormat.addInputPath(job2, new Path(outputPathStep1));
//			FileOutputFormat.setOutputPath(job2, new Path(outputPathStep2));
//			
//			fs = FileSystem.get(conf);
//			if (fs.exists(new Path(outputPathStep2))) {
//				fs.delete(new Path(outputPathStep2), true);
//			}
//
//			job2.waitForCompletion(true);
			
			//mr step 3
//			job.setNumReduceTasks(10);
//			job.setJarByClass(Driver.class);
//			job.setMapperClass(SimilarityMapper3.class);
//			job.setReducerClass(SimilarityReducer3.class);
//			
//			FileInputFormat.addInputPath(job, new Path(outputPathStep2));
//			String outputPathStep3 = "similarityStep3Out";
//			FileOutputFormat.setOutputPath(job, new Path(outputPathStep3));
//
//			job.waitForCompletion(true);
			
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
