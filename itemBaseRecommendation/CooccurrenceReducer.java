package itemBaseRecommendation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lib.ItemCooccurrence;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CooccurrenceReducer extends Reducer<Text,Text,Text,ItemCooccurrence>{
	
	@Override
	public void reduce(Text nid, Iterable<Text> targetNidList, Context context) {
		Map<Text,DoubleWritable> targetNidHash = new HashMap<Text,DoubleWritable>();
		
		for (Text targetNid : targetNidList) {
			if (targetNidHash.containsKey(targetNid)) {
				double count = targetNidHash.get(targetNid).get();
				targetNidHash.put(targetNid, new DoubleWritable(++count));
			}
			else {
				targetNidHash.put(targetNid, new DoubleWritable(1));
			}

		}
		
		ItemCooccurrence itemCooccurrence = new ItemCooccurrence(nid,targetNidHash);
		
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
