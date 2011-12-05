package similarity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer3 extends Reducer<IntWritable,Text,IntWritable,Text>{
	
	@Override
	public void reduce (IntWritable key, Iterable<Text> values, Context context) {
		
		
		Map<String,Integer> top5 = new HashMap<String,Integer>();
		int low = getlowest(top5);
		for (Text value : values) {
			String[] tmp = value.toString().split(",");
			String userId = tmp[0];
			int count = 0;
			try {
				count = Integer.parseInt(tmp[1]);
			}catch (Exception e) {
				continue;
			}
			
			if (top5.size()<5) {
				top5.put(userId, count);
				low = getlowest(top5);
			}
			else if (count >low) {
				replaceLowest(top5, userId, count);
			}
			
			
		}
		
		String top5users = "";
		for (Map.Entry<String, Integer> entry : top5.entrySet()) {
			if (top5users.equals(""))
				top5users = entry.getKey();
			else
				top5users += "," + entry.getKey();
		}
		
		try {
			context.write(key, new Text(top5users));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private void replaceLowest(Map<String,Integer> mapValue, String userId, int count) {
		int low = 0;
		String lowUserId = "";
		for (Map.Entry<String, Integer> entry : mapValue.entrySet()) {
			int value = entry.getValue().intValue();
			
			if (low == 0) {
				low = value;
				lowUserId = entry.getKey();
			}
			else if (value > low) {
				low = value;
				lowUserId = entry.getKey();
			}

		}
		mapValue.remove(lowUserId);
		mapValue.put(userId, count);
		
	}

	private int getlowest (Map<String,Integer> mapValue) {
		
		int lowest = 0;
		if (mapValue.size() == 0)
			return lowest;

		for (Map.Entry<String, Integer> entry : mapValue.entrySet()) {
			int value = entry.getValue().intValue();
			if (lowest == 0)
				lowest = value;
			else if (value < lowest)
				lowest = value;
		}
		
		return lowest;
	}
}
