package itemBaseRecommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class userVectorReducer extends Reducer<Text,Text,Text,ArrayWritable>{
	
	@Override
	public void reduce(Text uid, Iterable<Text> nidList, Context context) {

		List<String> list = new ArrayList<String>();
		for (Text t : nidList) {
			list.add(t.toString());
		}
		ArrayWritable aw = new ArrayWritable(new String[0]);
		
		try {

			context.write(uid, aw);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
