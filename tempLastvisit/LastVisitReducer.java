package tempLastvisit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import lib.DateUtil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastVisitReducer extends Reducer<Text,Text,Text,Text>{
	
	@Override
	public void reduce (Text uid, Iterable<Text> timeList, Context context) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss",Locale.US);
		Date max = null;
		for (Text time : timeList) {

			try {
				Date result = sdf.parse(time.toString());
				if (max == null || result.after(max)) {
					max = result;
				}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			context.write(uid, new Text(DateUtil.formatDate(max)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main (String[] args) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss",Locale.US);
		System.out.println(sdf.parse("2011-12-20:12:59:20"));
	}

}
