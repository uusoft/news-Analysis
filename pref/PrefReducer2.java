package pref;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrefReducer2 extends Reducer<Text,Text,Text,Text>{
	
	@Override
	public void reduce (Text uid, Iterable<Text> uidList, Context context) {
		Map<String,Integer> map = new HashMap<String,Integer>();
		for (Text targetUid : uidList) {
			String uidString = targetUid.toString();
			if (map.containsKey(uidString)) {
				int count = map.get(uidString).intValue();
				count++;
				map.put(uidString, new Integer(count));
			}
			else {
				map.put(uidString, new Integer(1));
			}
		}
		String result = "";
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			result += entry.getKey() + "," + entry.getValue() + "|";
		}
		try {
			context.write(uid, new Text(result));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

}
