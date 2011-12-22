package parse.receive;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseReducer extends Reducer<Text,Text,Text,Text>{
	
	@Override
	public void reduce(Text userId, Iterable<Text> termIdList, Context context) {
		
		HashSet<String> terms = new HashSet<String>();
		for (Text termId : termIdList) {
			if (!terms.contains(termId.toString())) {
				terms.add(termId.toString());
			}
		}
		
		for (String termId : terms) {
			try {
				context.write(userId, new Text(termId));
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
