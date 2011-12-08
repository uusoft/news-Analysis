package lib;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DateUtil {

	public static String formatDate(Date date) {
		String result = formatDate(date,"yyyy-MM-dd:hh:mm:dd");
		return result;
	}
	
	public static String formatDate(Date date, String format) {
		if (date == null)
			return "";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		String result = sdf.format(date);
		
		return result;

	}
	
	public static Date parseDateFromLog (String line) {
		
		String time = "";
		String regex_time = ".*\\[(.*)\\].*";
		Pattern pattern_time = Pattern.compile(regex_time);
		Matcher matcher_time = pattern_time.matcher(line);
		if (matcher_time.matches()) {
			time = matcher_time.group(1);
		}
		
		time = time.replaceAll("\\s", "");
		time = time.replaceAll("\\+0800", "");
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss",Locale.US);
		Date result = null;
		try {
			result = sdf.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static void main(String[] args) {
		String line = "10.13.81.241 - - [09/Nov/2011:23:59:01 +0800] GET /api/news/count.go?newsId=902882&p1=MjE1OTU2 HTTP/1.1";
		
		
		String dateString = "09/Nov/2011:23:59:01 +0800";
		dateString = dateString.replaceAll("\\s", "");
		dateString = dateString.replaceAll("\\+0800", "");
		System.out.println(dateString);
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMMM/yyyy:hh:mm:ss",Locale.US);
		try {
			Date d = sdf.parse(dateString);
			System.out.println(d);
//			String r = DateUtil.formatDate(DateUtil.parseDateFromLog(line));
//			System.out.println(r);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
