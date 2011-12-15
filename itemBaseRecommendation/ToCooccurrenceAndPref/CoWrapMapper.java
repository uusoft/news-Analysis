package itemBaseRecommendation.ToCooccurrenceAndPref;

import java.io.IOException;

import lib.CooccurrenceOrPrefWritable;
import lib.ItemCooccurrence;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoWrapMapper extends
		Mapper<Text, ItemCooccurrence, Text, CooccurrenceOrPrefWritable> {

	@Override
	public void map (Text nid, ItemCooccurrence co, Context context) {
		CooccurrenceOrPrefWritable cpw = new CooccurrenceOrPrefWritable();
		cpw.setItemCooccurrence(co);
		
		try {
			context.write(nid, cpw);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
