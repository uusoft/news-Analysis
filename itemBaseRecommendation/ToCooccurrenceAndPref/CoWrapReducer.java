package itemBaseRecommendation.ToCooccurrenceAndPref;

import java.io.IOException;

import lib.CooccurrenceOrPrefWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoWrapReducer
		extends
		Reducer<Text, CooccurrenceOrPrefWritable, Text, CooccurrenceOrPrefWritable> {

	@Override
	public void reduce (Text nid, Iterable<CooccurrenceOrPrefWritable> cpwList, Context context) {
		for (CooccurrenceOrPrefWritable cpw : cpwList) {
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
}
