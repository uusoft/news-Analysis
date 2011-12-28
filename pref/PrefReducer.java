package pref;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrefReducer extends Reducer<Text,Text,Text,Text>{ 
	@Override
	public void reduce (Text nid, Iterable<Text> uidList, Context context)  {
		Set<Text> set = new HashSet<Text>();
		List<Text> list = new ArrayList<Text>();
		for (Text uid : uidList) {
			set.add(new Text(uid.toString()));
		}
		for (Text uid : set) {
			list.add(uid);
		}
		if (list.size() < 2) 
			return;
		for (int i=0; i<list.size()-1; i++) {
			for (int j=i+1; j<list.size(); j++) {
				try {
					context.write(list.get(i), list.get(j));
					context.write(list.get(j), list.get(i));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

//			try {
//				context.write(list.get(i), list.get(i));
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		
	}
	
	public static void main (String[] args) {
		Set<Text> set = new HashSet<Text>();
		set.add(new Text("a"));
		set.add(new Text("b"));
		set.add(new Text("a"));
		
		for (Text t : set) {
			System.out.println(t);
		}
	}

}
