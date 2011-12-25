package test;

import java.io.IOException;
import java.util.Map;

import lib.JobUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TestMapWritableJob {

	public static class Mapper1 extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text nid, Text uidAndTime, Context context) {
			String[] tmp = uidAndTime.toString().split(",");
			if (tmp.length != 2)
				return;
			String uid = tmp[0];
			if (uid.equals(""))
				return;
			try {
				context.write(new Text(uid), nid);
				// System.out.println(uid+","+nid);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			ArrayWritable aw = null;
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text uid, Iterable<Text> valueList, Context context) {
			MapWritable preference = new MapWritable();
			
			for (Text nid : valueList) {
				Text newNid = new Text(nid.toString());
				if (preference.containsKey(newNid)) {
					IntWritable value = (IntWritable) preference.get(newNid);
					preference.put(newNid, new IntWritable(value.get()+1));
				}
				else
					preference.put(newNid, new IntWritable(1));
			}
			
			for (Map.Entry<Writable, Writable> entry : preference.entrySet()) {
				Text k = (Text) entry.getKey();
				IntWritable v = (IntWritable) entry.getValue();
				
				try {
					context.write(new Text(uid+":"+k), new Text(v.get()+""));
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

	public static void main(String[] args) {
		
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		try {
			Job job = JobUtil.prepareJob("test", conf, Mapper1.class, Text.class,
					Text.class, Reducer1.class, Text.class, Text.class);
			job.setJarByClass(TestMapWritableJob.class);
//			job.setInputFormatClass(TextInputFormat.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
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
