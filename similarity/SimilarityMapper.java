package similarity;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	@Override
	public void map (Object key, Text value, Context context) {
		//value : newsid	userid,clicktime
		String[] tmp = value.toString().split(",");
		if (tmp.length <2)
			return;
		String[] tmp2 = tmp[0].split("\t");
		if (tmp2.length <2)
			return;
		String newsId = tmp2[0];
		int userId = 0;
		try {
			userId = Integer.parseInt(tmp2[1]);
		}catch (Exception e) {
			return;
		}
		
		try {
//			System.out.println(newsId+","+userId);
			context.write(new Text(newsId), new IntWritable(userId));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
