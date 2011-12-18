package itemBaseRecommendation2;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class WrapPrefMapper extends Mapper<Text,MapWritable,Text,CoAndPref>{
	
	@Override
	public void map (Text uid, MapWritable mw, Context context) {
		for (Map.Entry<Writable, Writable> entry : mw.entrySet()) {
			Text nid = (Text) entry.getKey();
			IntWritable count = (IntWritable) entry.getValue();
			MapWritable pref = new MapWritable();
			pref.put(uid, count);
			CoAndPref cap = new CoAndPref();
			cap.setNid(nid);
			cap.setPref(mw);
			cap.setHasPref(new BooleanWritable(true));
			
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
