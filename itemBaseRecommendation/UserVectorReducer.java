package itemBaseRecommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lib.ListWritable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;


public class UserVectorReducer extends Reducer<Text,Text,Text,ListWritable>{
	
	@Override
	public void reduce(Text uid, Iterable<Text> nidList, Context context) {

		List<Text> list = new ArrayList<Text>();
		for (Text t : nidList) {
			list.add(t);
		}
		ListWritable lw = new ListWritable(Text.class);
		lw.set(list);
		
		
		try {

			context.write(uid, lw);
			
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


