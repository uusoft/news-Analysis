package parse.lastvisit;

import java.io.IOException;

import lib.ParseLog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author cuitao output: key=newsId value=p1 + "," + newsId + "," + time
 */
public class LastVisitMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) {

		String line = value.toString();
		String[] temp = ParseLog.parseInterface(line);
		if (temp == null)
			return;
		String interfaceName = temp[0];
		String p1 = temp[1];
		String newsId = temp[2];
		String time = temp[3];

//		if (p1.equalsIgnoreCase("12223618")) {
//			System.out.println(interfaceName+":"+p1 + "," + newsId + "," + time);
//		}
//		else {
//			return;
//		}
		
		try {
			context.write(new Text(p1), new Text(time));
		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}
	
	
}
