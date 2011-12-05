package sequenceFileTest;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SequenceFileTestReducer extends Reducer<Text,Text,Text,Text>{
	
	@Override
	public void reduce(Text num, Iterable<Text> lines, Context context) {
		for (Text line : lines) {
			try {
				context.write(num, line);
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
