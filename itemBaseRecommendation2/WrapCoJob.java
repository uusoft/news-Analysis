package itemBaseRecommendation2;

import java.io.IOException;

import lib.ItemCooccurrence;
import lib.JobUtil;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WrapCoJob extends ToolJob{

	/**
	 * @author cuitao
	 * a reducer that do nothing
	 */
	public static class WrapCoReducer extends Reducer<Text,CoAndPref,Text,CoAndPref> {
		@Override
		public void reduce(Text nid, Iterable<CoAndPref> list, Context context) {
			for (CoAndPref cap : list) {
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
	}


	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		try {
			Job job = JobUtil.prepareJob("WrapCoJob", conf, WrapCoMapper.class, Text.class, CoAndPref.class, WrapCoReducer.class, Text.class, CoAndPref.class);
			job.setNumReduceTasks(10);
			job.setJarByClass(WrapCoJob.class);
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
		
		return 1;
	}
	public static class WrapCoMapper extends Mapper<Text,ItemCooccurrence,Text,CoAndPref> {
		@Override
		public void map (Text nid, ItemCooccurrence ic, Context context) {
			CoAndPref cap = new CoAndPref();
			cap.setCo(ic.getCooccurrence());
			cap.setHasCo(new BooleanWritable(true));
			cap.setNid(nid);
			
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
	
	
}
