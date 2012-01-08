package test;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GetNidList {

	public static class GetNidListMapper extends Mapper<Text,Text,Text,Text> {
		@Override
		public void map (Text nid, Text value, Context context) {
			try {
				context.write(nid, new Text(""));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class GetNidListReducer extends Reducer <Text,Text,Text,Text> {
		@Override
		public void reduce (Text nid, Iterable<Text> list, Context context) {
			try {
				context.write(nid, new Text(""));
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
