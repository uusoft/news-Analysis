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
			if (nidMap.containsKey(nid2)) {
				IntWritable coValue = (IntWritable) nidMap.get(nid2);
				int count = coValue.get();
				nidMap.put(nid2, new IntWritable(++count));
			}
			else {
				nidMap.put(nid2, new IntWritable(1));
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
