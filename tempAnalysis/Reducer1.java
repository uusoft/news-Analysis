package tempAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	@Override
	public void reduce(Text uidAndDay, Iterable<IntWritable> countList, Context context) {
		int sum = 0;
		for (IntWritable count : countList) {
			sum++;
		}
		
		try {
			context.write(uidAndDay, new IntWritable(sum));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
