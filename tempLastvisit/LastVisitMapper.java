package tempLastvisit;

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

		String[] temp = ParseLog.parseInterface(value.toString());
		if (temp == null)
			return;
		String interfaceName = temp[0];
		String p1 = temp[1];
		String newsId = temp[2];
		String time = temp[3];

		if (p1.equalsIgnoreCase("12223618")) {
			try {
				context.write(new Text(p1), value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			return;
		}
		


	}
	
	
}
