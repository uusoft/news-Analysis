package similarity;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper3 extends Mapper<Text,IntWritable,IntWritable,Text>{ 
	
	@Override
	public void map (Text key, IntWritable value, Context context) {
		String tmp[] = key.toString().split(",");
		if (tmp.length != 2) 
			return;
		int userId1,userId2;
		try {
			userId1 = Integer.parseInt(tmp[0]);
			userId2 = Integer.parseInt(tmp[1]);
		}catch (Exception e) {
			return;
		}
		try {
			context.write(new IntWritable(userId1), new Text(userId2 + "," + value.get()));
			context.write(new IntWritable(userId2), new Text(userId1 + "," + value.get()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
