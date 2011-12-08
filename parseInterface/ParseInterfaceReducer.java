package parseInterface;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseInterfaceReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text interfaceName, Iterable<Text> list, Context context) {
		for (Text value : list) {
			try {
				context.write(interfaceName, value);
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
