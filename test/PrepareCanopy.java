package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import lib.JobUtil;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

public class PrepareCanopy extends ToolJob{
	
	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		
		Configuration conf = getConf();
		Job job = JobUtil.prepareJob("prepare canopy", conf, Mapper1.class, Text.class, Text.class, Reducer1.class, Text.class, VectorWritable.class);
		
		job.setJarByClass(PrepareCanopy.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		return 0;
	}

	public static class Mapper1 extends Mapper<Text,Text,Text,Text> {
		@Override
		public void map (Text nid, Text line, Context context) {
			String[] tmp2 = line.toString().split("\t");
			if (tmp2.length <1) 
				return;
			String uid = tmp2[0];
			
			if (uid.equalsIgnoreCase("") || uid.length()<=3)
				return;
			
			try {
				context.write(new Text(uid), nid);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
//	public static class Mapper1 extends Mapper<Object,Text,Text,Text> {
//		@Override
//		public void map (Object object, Text line, Context context) {
//			String[] tmp2 = line.toString().split("\t");
//			if (tmp2.length !=2) 
//				return;
//			Text nid = new Text(tmp2[0]);
//			String[] tmp3 = tmp2[1].toString().split(",");
//			if (tmp3.length != 2) {
//				return;
//			}
//			String uid = tmp3[0];
//			if (uid.equalsIgnoreCase("") || uid.length()<2)
//				return;
//			
//			try {
//				context.write(new Text(uid), nid);
//			} catch (IOException e) {
//				e.printStackTrace();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}
	
	public static class Reducer1 extends Reducer<Text,Text,Text,VectorWritable> {
		
		private SortedMap<String,Integer> uidMap = new TreeMap<String,Integer>();
		
		@Override
		public void setup (Context context) {
			try {
				FileReader filereader = new FileReader("uniqueNid.txt");
				BufferedReader br = new BufferedReader(filereader);
				String nid = null;
				while ( (nid = br.readLine()) != null) {
					uidMap.put(nid, 1);
				}
				filereader.close();
				br.close();
				
				int i=0;
				for (Map.Entry<String, Integer> entry : uidMap.entrySet()) {
					entry.setValue(i++);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		@Override
		public void reduce (Text uid, Iterable<Text> nidList, Context context) {
			SortedMap<String,Integer> map = new TreeMap<String,Integer>();
//			System.out.println("uid:"+uid);
			if (uid.toString().contains("10000217")) {
//				System.out.println("ok");
			}
			for (Text nid : nidList) {
//				System.out.println("nid:"+nid);
				if (map.containsKey(nid.toString())) {
					int count = map.get(nid.toString()).intValue();
					count++;
					map.put(nid.toString(), count);
				}
				else {
					map.put(nid.toString(), 1);
				}
			}
			
//			List<Integer> pref = new ArrayList<Integer>(uidMap.size());
//			for (Map.Entry<String, Integer> entry : map.entrySet()) {
//				String nid = entry.getKey();
//				pref.set(uidMap.get(nid), entry.getValue());
//				
//			}
			double[] pref = new double[uidMap.size()];
			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				String nid = entry.getKey();
				pref[uidMap.get(nid)] = entry.getValue();
				
			}
//			ArrayWritable aw = new ArrayWritable();
			RandomAccessSparseVector r = new RandomAccessSparseVector(uidMap.size(),uidMap.size());
			r.assign(pref);
			VectorWritable v = new VectorWritable(r);
			
			try {
				context.write(uid, v);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main (String[] args) {
		
		double[] a = new double[] {1,2,0,0,0,3,1.5,0,4,5,0};

		RandomAccessSparseVector r = new RandomAccessSparseVector(a.length,a.length);
		r.assign(a);
		
		System.out.println(r);
		System.out.println(r.get(2));
		
	}

	





















}
