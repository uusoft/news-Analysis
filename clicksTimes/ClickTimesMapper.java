package clicksTimes;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ClickTimesMapper extends Mapper<Object,Text,IntWritable,IntWritable> {

	@Override
	public void map(Object key, Text value, Context context) {
		//value : userid	newsid	clicktime
		String[] tmp = value.toString().split(",");
		if (tmp.length <2)
			return;
		
		int userId = 0;
		try {
			userId = Integer.parseInt(tmp[0]);
		}catch (Exception e) {
			return;
		}
		try {
			context.write(new IntWritable(userId), new IntWritable(1));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
