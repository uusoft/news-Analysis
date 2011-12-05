package itemBaseRecommendation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class userVectorMapper extends Mapper<Text,Text,Text,Text>{
	
	@Override
	public void map(Text nid, Text uidAndTime, Context context) {
		String[] tmp = uidAndTime.toString().split(",");
		if (tmp.length != 2)
			return;
		String uid = tmp[0];
		if (uid.equals(""))
			return;
		try {
			context.write(new Text(uid), nid);
			System.out.println(uid+","+nid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
