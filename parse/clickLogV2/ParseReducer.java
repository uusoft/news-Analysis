package parse.clickLogV2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text newsId, Iterable<Text> userAndTimeList, Context context) {
		for (Text userAndTime : userAndTimeList) {
			try {
				context.write(newsId, userAndTime);
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
