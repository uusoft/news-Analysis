package parseInterface;
import java.io.IOException;

import lib.ParseLog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author cuitao output: key=newsId value=p1 + "," + newsId + "," + time
 */
public class ParseInterfaceMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) {

		String[] temp = ParseLog.parseInterface(value.toString());
		if (temp == null)
			return;
		String interfaceName = temp[0];
		String p1 = temp[1];
		String newsId = temp[2];
		String time = temp[3];
		String tmp[] = time.split(":", 2);
		String date = "";
		if (tmp.length == 2) {
			date  = tmp[0];
			time = tmp[1];
		}
		
		
		
		try {
			context.write(new Text(interfaceName), new Text(p1 + "," + newsId + "," + date + "," + time));
		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}
	
	
}
