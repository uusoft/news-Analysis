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
	public void reduce(Text nid, Iterable<Text> nidList, Context context) {
		Map<Text,DoubleWritable> nidHash = new HashMap<Text,DoubleWritable>();
		
		for (Text nid2 : nidList) {
			double count = nidHash.get(nid2).get();
			nidHash.put(nid2, new DoubleWritable(++count));
			
		}
		
		ItemCooccurrence itemCooccurrence = new ItemCooccurrence(nid,nidHash);
		
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
