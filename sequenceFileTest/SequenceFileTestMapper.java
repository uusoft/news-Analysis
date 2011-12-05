package sequenceFileTest;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceFileTestMapper extends Mapper<Object,Text,Text,Text>{
	
	@Override
	public void map (Object num,Text line, Context context) {
		
		try {
			context.write(new Text("1"), line);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
