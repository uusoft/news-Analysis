package itemBaseRecommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

		MapWritable preference = new MapWritable();

		for (Text nid : nidList) {
			if (preference.containsKey(nid)) {
				IntWritable value = (IntWritable) preference.get(nid);
				preference.put(nid, new IntWritable(value.get()+1));
			}
			else
				preference.put(nid, new IntWritable(1));
		}
		
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
		List<Text> list = new ArrayList<Text>();
		list.add(new Text("a"));
		list.add(new Text("b"));
		list.add(new Text("c"));
		list.add(new Text("d"));
		
		ListWritable lw = new ListWritable(Text.class);
		lw.set(list);
		
		for (Writable w : lw.getList()) {
			Text t = (Text) w;
			System.out.println(t.toString());
		}
		System.out.println(lw);
	}

}


