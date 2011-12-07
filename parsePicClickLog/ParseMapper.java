package parsePicClickLog;

import java.io.IOException;

import lib.ParseLog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author cuitao output: key=newsId value=userId + "," + time
 */
public class ParseMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) {

		String firstColumn = value.toString().split("\"")[0];
		String[] temp = ParseLog.parsePicLog(firstColumn);
		if (temp == null)
			return;
		String picName = temp[0];
		String userId = temp[1];
		String time = temp[2];
		
		try {
			context.write(new Text(picName), new Text(userId + "," + time));
		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}
	
	
}
