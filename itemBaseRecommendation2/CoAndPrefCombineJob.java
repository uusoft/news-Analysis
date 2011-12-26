package itemBaseRecommendation2;

import itemBaseRecommendation2.WrapCoJob.WrapCoMapper;
import itemBaseRecommendation2.WrapCoJob.WrapCoReducer;

import java.io.IOException;

import lib.JobUtil;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CoAndPrefCombineJob extends ToolJob{

	@Override
	public int run(String[] args) throws Exception {

		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		try {
			Job job = JobUtil.prepareJob("capCombined", conf, CAPCombineMapper.class, Text.class, CoAndPref.class, CAPCombineReducer.class, Text.class, CoAndPref.class);
			job.setNumReduceTasks(10);
			job.setJarByClass(CoAndPrefCombineJob.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, new Path(input));
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
		
		return 0;
	}
	
	private static class CAPCombineMapper extends Mapper<Text,CoAndPref,Text,CoAndPref> {
		@Override
		public void map (Text nid, CoAndPref cap, Context context) {
			
			try {
				context.write(nid, cap);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	private static class CAPCombineReducer extends Reducer<Text,CoAndPref,Text,CoAndPref> {
		@Override
		public void reduce (Text nid, Iterable<CoAndPref> list, Context context) {
			CoAndPref capCombined = new CoAndPref();
			capCombined.setNid(nid);
			for (CoAndPref cap : list) {
				if (cap.getHasCo().get()) {
					capCombined.setCo(cap.getCo());
				}
				
				if (cap.getHasPref().get()) {
					capCombined.setPref(cap.getPref());
				}
			}
			
			try {
				context.write(nid, capCombined);
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
