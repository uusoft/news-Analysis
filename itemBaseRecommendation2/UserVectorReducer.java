package itemBaseRecommendation2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import lib.ListWritable;
import lib.TextListWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;


public class UserVectorReducer extends Reducer<Text,Text,Text,MapWritable>{
	
	@Override
	public void reduce(Text uid, Iterable<Text> nidList, Context context) {

//		System.out.println("hello");
		MapWritable preference = new MapWritable();
		List<Text> t = new ArrayList<Text>();

		for (Text nid : nidList) {
			t.add(nid);
			if (preference.containsKey(nid)) {
				IntWritable value = (IntWritable) preference.get(nid);
				preference.put(nid, new IntWritable(value.get()+1));
			}
			else
				preference.put(nid, new IntWritable(1));
		}
//		
//		for (Entry<Writable, Writable> entry : preference.entrySet()) {
////			System.out.println(entry.getKey()+","+entry.getValue());
//		}
		try {

			context.write(uid, preference);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main (String[] args) {
		MapWritable mw = new MapWritable();
		mw.put(new Text("a"), new IntWritable(1));
		mw.put(new Text("b"), new IntWritable(1));
		mw.put(new Text("a"), new IntWritable(2));
		if (mw.containsKey(new Text("a")))
			System.out.println("get");
		for (Entry<Writable, Writable> entry : mw.entrySet()) {
			Text key = (Text) entry.getKey();
			IntWritable value = (IntWritable) entry.getValue();
			System.out.println(key.toString()+","+value.get());
		}
	}

}


