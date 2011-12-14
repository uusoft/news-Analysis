package stat.api;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class UvReducer extends Reducer<Text,Text ,Text,Text>{
	
	Set<String> uv = new HashSet<String>();
	@Override
	public void reduce (Text apiName, Iterable<Text> uidList, Context context) {
		for (Text uid : uidList)
			uv.add(uid.toString());
				
		for (String uid : uv) {
			try {
				context.write(new Text(uid), new Text("1"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main (String[] args) {
		Set<String> uv = new HashSet<String>();
		uv.add("a");
		uv.add("a");
		uv.add("b");
		
		for (String s : uv) {
			System.out.println(s);
		}
		
	}
	


}
