package test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TextListWritableTest {
	
	public static class Mapper1 extends Mapper<Text,Text,Text,Text> {
		
		@Override
		public void map (Text key, Text value, Context context) {
			
		}
	}
	
	public static class Reducer1 extends Reducer<Text,Text,Text,Text> {
		
	}

}
