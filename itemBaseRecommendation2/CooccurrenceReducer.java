package itemBaseRecommendation2;

import java.io.IOException;

import lib.ItemCooccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CooccurrenceReducer extends Reducer<Text,Text,Text,ItemCooccurrence>{
	
	@Override
	public void reduce(Text nid, Iterable<Text> nidList, Context context) {
		MapWritable nidMap = new MapWritable();
		
		for (Text nid2 : nidList) {
			Text key = new Text(nid2.toString());
			if (nidMap.containsKey(key)) {
				IntWritable coValue = (IntWritable) nidMap.get(key);
				int count = coValue.get();
				nidMap.put(key, new IntWritable(++count));
			}
			else {
				nidMap.put(key, new IntWritable(1));
			}

		}
		
		ItemCooccurrence itemCooccurrence = new ItemCooccurrence(nid,nidMap);
		
		try {
			context.write(nid, itemCooccurrence);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
