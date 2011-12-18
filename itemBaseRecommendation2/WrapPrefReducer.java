package itemBaseRecommendation2;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WrapPrefReducer extends Reducer<Text,CoAndPref,Text,CoAndPref>{
	
	@Override
	public void reduce(Text nid, Iterable<CoAndPref> capList, Context context) {
		for (CoAndPref cap : capList) {
			try {
				context.write(nid, cap);
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
