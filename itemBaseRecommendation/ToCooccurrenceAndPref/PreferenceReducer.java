package itemBaseRecommendation.ToCooccurrenceAndPref;

import java.util.Map.Entry;

import lib.CooccurrenceOrPrefWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class PreferenceReducer
		extends
		Reducer<Text, CooccurrenceOrPrefWritable, Text, CooccurrenceOrPrefWritable> {

	@Override
	public void reduce(Text nid, Iterable<CooccurrenceOrPrefWritable> cpwList, Context context) {
		MapWritable pref = new MapWritable();
		for (CooccurrenceOrPrefWritable cpw : cpwList) {
			MapWritable mw = cpw.getUserPrefMap();
			pref.putAll(mw);
		}
	}
	
	
	public static void main (String[] args) {
		MapWritable pref = new MapWritable();
		MapWritable t1 = new MapWritable();
		MapWritable t2 = new MapWritable();
		t1.put(new Text("a"), new IntWritable(1));
		t1.put(new Text("b"), new IntWritable(2));
		t2.put(new Text("a"), new IntWritable(3));
		t2.put(new Text("c"), new IntWritable(4));
		
		pref.putAll(t1);
		pref.putAll(t2);
		
		for (Entry<Writable, Writable> entry : pref.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}
}


