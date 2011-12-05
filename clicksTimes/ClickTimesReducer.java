package clicksTimes;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author cuitao
 * 输出结果: 每个userId的clicks数
 */
public class ClickTimesReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) {
		int clicks = 0;
		for (IntWritable value : values) {
			clicks += value.get();
		}
		
		try {
			context.write(key, new IntWritable(clicks));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
