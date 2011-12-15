package itemBaseRecommendation.ToCooccurrenceAndPref;

import java.io.IOException;
import java.util.Map.Entry;

import lib.CooccurrenceOrPrefWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class PreferenceMapper extends
		Mapper<Text, MapWritable, Text, CooccurrenceOrPrefWritable> {

	@Override
	public void map(Text uid, MapWritable pref, Context context) {
		for (Entry<Writable,Writable> entry: pref.entrySet()) {
			Text nid =  (Text) entry.getKey();
//			IntWritable prefValue = (IntWritable) entry.getValue();
			MapWritable mw = new MapWritable();
			mw.put(uid, entry.getValue());
			CooccurrenceOrPrefWritable cpw = new CooccurrenceOrPrefWritable();
			cpw.setMapWritable(pref);
			
			try {
				context.write(nid, cpw);
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
