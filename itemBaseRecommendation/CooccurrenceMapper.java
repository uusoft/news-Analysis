package itemBaseRecommendation;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class CooccurrenceMapper extends Mapper<Text,MapWritable,Text,Text>{
	
	@Override
	public void map(Text uid,MapWritable prefMap, Context context) {
		
		Text[] nidArray = prefMap.keySet().toArray(new Text[0]);
		
		for (int i=0; i<nidArray.length; i++) {
			for (int j=i+1; j<nidArray.length; j++) {
				try {
					context.write( nidArray[i], nidArray[j]);
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

}
