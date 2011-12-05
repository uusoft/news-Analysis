package similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author cuitao
 *输出:点击了同一个新闻的用户对:userId1,userId2	1
 */
public class SimilarityReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	@Override
	public void reduce (Text key, Iterable<IntWritable> values, Context context) {

		List<Integer> userIdList = new ArrayList<Integer>();
		for (IntWritable value : values ) {
			userIdList.add(value.get());
		}
		
		for (int i=0; i<userIdList.size()-1; i++) {
//				System.out.println("calculate:"+i);
			for (int j=i+1; j<userIdList.size(); j++) {
				try {
					int userId1 = userIdList.get(i).intValue();
					int userId2 = userIdList.get(j).intValue();
					if (userId1 == userId2) 
						continue;
					String outputKey = userId1 + "," + userId2;
					
					context.write(new Text(outputKey), new IntWritable(1));
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

}
