package pvuv;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author cuitao
 *读取点击日志
 */
public class PvuvMapper extends Mapper<Object, Text, Text,IntWritable>{

	@Override
	public void map(Object key, Text value, Context context) {
		//value : userid	newsid	clicktime
		String tmp[] = value.toString().split(",");
		if (tmp.length < 2) 
			return;
		String userid = tmp[0];
		String newsid = tmp[1];
		
		int useridInt = 0;
		try {
			useridInt = Integer.parseInt(userid);
		}catch (Exception e) {
			return;
		}
		
		try {
			//key:newsid	value:userid
			context.write(new Text(newsid), new IntWritable(useridInt));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
