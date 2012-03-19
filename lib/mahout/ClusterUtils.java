package lib.mahout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.WeightedVectorWritable;


public class ClusterUtils {

	public static Map<Text,Cluster> readClusters(Path file, Configuration conf) {
		Map<Text,Cluster> clusterMap = new HashMap<Text,Cluster>();
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			Text key = new Text();
			
			Cluster cluster = (Cluster) reader.getValueClass().newInstance();
			
			while (reader.next(key, cluster)) {
				clusterMap.put(key, cluster);
			}
			reader.close();
			fs.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return clusterMap;
	}
	
	public static Map<IntWritable,WeightedVectorWritable> readPoints(Path file, Configuration conf) {
		Map<IntWritable,WeightedVectorWritable> map = new HashMap<IntWritable,WeightedVectorWritable>();
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			IntWritable key = new IntWritable();
			
			WeightedVectorWritable WeightedVectorWritable = (WeightedVectorWritable) reader.getValueClass().newInstance();
			
			while (reader.next(key, WeightedVectorWritable)) {
				map.put(key, WeightedVectorWritable);
			}
			reader.close();
			fs.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return map;
	}
	
	
	public static void main (String[] args) {
		Map<Text,Cluster> map = readClusters(new Path("/home/cuitao/workspace/kmeansCluster"),new Configuration());
		
		Map<IntWritable,WeightedVectorWritable> map2 = readPoints(new Path("/home/cuitao/workspace/kmeansPoints"),new Configuration());
		System.out.println("read over");
		
		
		
		
	}
}
