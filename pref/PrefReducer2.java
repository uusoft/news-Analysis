package pref;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrefReducer2 extends Reducer<Text,Text,Text,Text>{
	
	@Override
	public void reduce (Text uid, Iterable<Text> uidList, Context context) {
		Map<Text,Integer> map = new HashMap<Text,Integer>();
		for (Text targetUid : uidList) {
			if (map.containsKey(targetUid)) {
				int count = map.get(targetUid).intValue();
				count++;
				map.put(targetUid, new Integer(count));
			}
			else {
				map.put(targetUid, new Integer(1));
			}
			String result = "";
			for (Map.Entry<Text, Integer> entry : map.entrySet()) {
				result = entry.getKey().toString()+","+entry.getValue().intValue()+"|";
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

}
