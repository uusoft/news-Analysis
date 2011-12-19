package itemBaseRecommendation2;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class CooccurrenceMapper extends Mapper<Text,MapWritable,Text,Text>{
	
	@Override
	public void map(Text uid,MapWritable mw, Context context) {
		
		Set<Writable>  set = mw.keySet();
		Text[] array = set.toArray(new Text[0]);
		if (array.length >1) {
			System.out.println("hello");
		}
		for (int i=0; i<array.length; i++) {
			for (int j=i+1; j<array.length; j++) {
				try {
					context.write(array[i], array[j]);
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
