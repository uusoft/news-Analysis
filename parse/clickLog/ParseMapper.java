package parse.clickLog;

import java.io.IOException;

import lib.ParseLog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author cuitao 
 */
public class ParseMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) {
		String line = value.toString();
		String firstColumn = line.split("\"")[0];
		
		String[] parsedList = ParseLog.parseHtmlLog(firstColumn);
		if (parsedList == null)
			return;
		String newsId = parsedList[0];
		String uid = parsedList[1];
		String time = parsedList[2];
		
		try {
			context.write(new Text(newsId), new Text(uid + "," + time));
		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}


}
