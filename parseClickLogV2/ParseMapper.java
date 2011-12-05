package parseClickLogV2;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lib.ParseLog;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author cuitao output: key=newsId value=userId + "," + time
 */
public class ParseMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) {

		String firstColumn = value.toString().split("\"")[0];
		String[] temp = ParseLog.parseInterfaceOfArticle(firstColumn);
		if (temp == null)
			return;
		String newsId = temp[0];
		String userId = temp[1];
		String time = temp[2];
		
		try {
			context.write(new Text(newsId), new Text(userId + "," + time));
		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}
	
	
}
