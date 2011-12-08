package lib;

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;

public class ParseLog {
	
	/**
	 * parse first column of nginx log with article.go interface
	 * @param line
	 * @return String array with 3 column:
	 * result[0] = newsId;
	 * result[1] = p1;
	 * result[2] = time;
	 */
	public static String[] parseInterfaceOfArticle(String line) {
		String[] result = new String[3];
		String newsId="";
		
		{
			//check article.go
			String regex_article = ".*(/api/news/article\\.go.*).*";
			Pattern pattern_article = Pattern.compile(regex_article);
			Matcher matcher = pattern_article.matcher(line);
			if (!matcher.matches()) {
				return null;
			}
		}
		
		//parse newsId
		String regex_newsId = ".*newsId=([\\d]*).*";
		Pattern pattern_newsId = Pattern.compile(regex_newsId);
		Matcher matcher_newsId = pattern_newsId.matcher(line);
		if (matcher_newsId.matches()) {
			newsId = matcher_newsId.group(1);
//			System.out.println(newsId);
		}
	

		//parse p1
		String p1 = "";
		String regex_p1 = ".*p1=([^\\s&]*).*";
		Pattern pattern_p1 = Pattern.compile(regex_p1);
		Matcher matcher2 = pattern_p1.matcher(line);
		if (matcher2.matches()) {
			p1 = matcher2.group(1);

			try {
				p1 = new String(Base64.decodeBase64(URLDecoder.decode(
						p1, "utf-8").getBytes("utf-8")));
			} catch (Exception e) {

				return null;
			}

		}
		
		//
		String time = DateUtil.formatDate(DateUtil.parseDateFromLog(line));
		
		result[0] = newsId;
		result[1] = p1;
		result[2] = time;
		
		return result;
	}

	
	/**
	 * parse first column of nginx log with *.html log
	 * @param line
	 * @return String array with 3 column:
	 * result[0] = newsId;
	 * result[1] = p1;
	 * result[2] = time;
	 */
	public static String[] parseHtmlLog(String line) {
		
		//check html
		String regex_html = ".*(/mpaper/.*\\.html).*";
		Pattern pattern_html = Pattern.compile(regex_html);
		Matcher matcher = pattern_html.matcher(line);
		String newsId = "";
		if (matcher.matches()) {
			// parse news id
			try {
				String matchedContent = matcher.group(1);
				String[] temp1 = matchedContent.split("/");
				String t = temp1[temp1.length - 1];
				String[] temp3 = t.split("\\.");
				String[] temp2 = temp3[0].split("_");
				newsId = temp2[temp2.length - 2];
				// logger.info(temp3[0]);
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();

				return null;
			}

		} else {
			return null;
		}
		
		String[] result = new String[3];

		//parse p1
		String p1 = "";
		String regex_p1 = ".*p1=([^\\s&]*).*";
		Pattern pattern_p1 = Pattern.compile(regex_p1);
		Matcher matcher2 = pattern_p1.matcher(line);
		if (matcher2.matches()) {
			p1 = matcher2.group(1);

			try {
				p1 = new String(Base64.decodeBase64(URLDecoder.decode(
						p1, "utf-8").getBytes("utf-8")));
			} catch (Exception e) {

				return null;
			}

		}
		
		//parse time
		String time = DateUtil.formatDate(DateUtil.parseDateFromLog(line));
		
		result[0] = newsId;
		result[1] = p1;
		result[2] = time;
		
		return result;
	}
	
	/**
	 * parse first column of nginx log with "/img7/ log
	 * @param line
	 * @return String array with 3 column:
	 * result[0] = picname;
	 * result[1] = p1;
	 * result[2] = time;
	 */
	public static String[] parsePicLog(String line) {
		
		//check /img7/
		String regex_img = ".*/img7/.*";
		Pattern pattern_img = Pattern.compile(regex_img);
		Matcher matcher = pattern_img.matcher(line);
		String picName = "";
		if (!matcher.matches()) 
			return null;
		
		String regex_pic = ".*/([\\d_]*)\\.(jpg|png|jpeg|gif).*";
		Pattern pattern_pic = Pattern.compile(regex_pic);
		Matcher matcher_pic = pattern_pic.matcher(line);
		if (matcher_pic.matches()) {
			
			// parse picName
			try {
				String matchedContent = matcher_pic.group(1);
				
				String[] temp1 = matchedContent.split("_");
				if (temp1.length == 0) {
					return null;
				}
				else {
					picName = temp1[0];
				}

			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();

				return null;
			}

		} else {

		}
		
		String[] result = new String[3];

		//parse p1
		String p1 = "";
		String regex_p1 = ".*p1=([^\\s&]*).*";
		Pattern pattern_p1 = Pattern.compile(regex_p1);
		Matcher matcher2 = pattern_p1.matcher(line);
		if (matcher2.matches()) {
			p1 = matcher2.group(1);

			try {
				p1 = new String(Base64.decodeBase64(URLDecoder.decode(
						p1, "utf-8").getBytes("utf-8")));
			} catch (Exception e) {

				return null;
			}

		}
		
		//parse time
		String time = DateUtil.formatDate(DateUtil.parseDateFromLog(line));
		
		result[0] = picName;
		result[1] = p1;
		result[2] = time;
		
		return result;
	}

	/**
	 * parse first column of nginx log with *.go interface expcept article.go
	 * @param line
	 * @return String array with 3 column:
	 * result[0] = goName;
	 * result[1] = p1;
	 * result[2] = time;
	 */
	public static String[] parseInterfaceExceptArticle(String line) {
		String[] result = new String[3];
		String goName="";
		
		//check go type and parse go name
		String regex_Allgo = ".*(/api/.*\\.go).*";
		Pattern pattern_Allgo = Pattern.compile(regex_Allgo);
		Matcher matcher_Allgo = pattern_Allgo.matcher(line);
		if (matcher_Allgo.matches()) {

			// match if it is article.go
			String regex_Articlego = ".*(/api/.*article\\.go.*).*";
			Pattern pattern_Articlego = Pattern.compile(regex_Articlego);
			Matcher matcher_Articlego = pattern_Articlego.matcher(line);
			if (matcher_Articlego.matches()) {
				// match article.go
				return null;
			}

			goName = matcher_Allgo.group(1);

		} else {
			// do not match *.go
			return null;
		}


		//parse p1
		String p1 = "";
		String regex_p1 = ".*p1=([^\\s&]*).*";
		Pattern pattern_p1 = Pattern.compile(regex_p1);
		Matcher matcher2 = pattern_p1.matcher(line);
		if (matcher2.matches()) {
			p1 = matcher2.group(1);

			try {
				p1 = new String(Base64.decodeBase64(URLDecoder.decode(
						p1, "utf-8").getBytes("utf-8")));
			} catch (Exception e) {

				return null;
			}

		}
		
		//parse time
		String time = DateUtil.formatDate(DateUtil.parseDateFromLog(line));
		
		result[0] = goName;
		result[1] = p1;
		result[2] = time;
		
		return result;
	}
	
	

	/**
	 * parse first column of nginx log with *.go/*.do
	 * @param line
	 * @return String array with 3 column:
	 * result[0] = interfaceName;
	 * result[1] = p1;
	 * result[2] = newsId;
	 * result[4] = time;
	 */
	public static String[] parseInterface(String line) {
		String[] result = new String[4];
		String interfaceName="";
		String newsId="";
		String time = "";
		String p1 = "";
		
		String firstColumn = line.split("\"")[0];
		//check go type and parse go name
//		String regex_Allgo = "..*(/api/.*\\.[g|d]?o).*";
		String regex_Allgo = "..*(/api/[\\d\\w/]*\\.[g|d]?o).*";
		Pattern pattern_Allgo = Pattern.compile(regex_Allgo);
		Matcher matcher_Allgo = pattern_Allgo.matcher(firstColumn);
		if (matcher_Allgo.matches()) {
			interfaceName = matcher_Allgo.group(1);
		} else {
			// do not match *.go
			return null;
		}
		
		//parse time
		time = DateUtil.formatDate(DateUtil.parseDateFromLog(firstColumn));
		
		//parse newsId

		String regex_newsId = ".*newsId=([\\d]*).*";
		Pattern pattern_newsId = Pattern.compile(regex_newsId);
		Matcher matcher_newsId = pattern_newsId.matcher(firstColumn);
		if (matcher_newsId.matches()) {
			newsId = matcher_newsId.group(1);
			// System.out.println(newsId);
		}
		

		//parse p1
		String regex_p1 = ".*b=([\\d]*).*";
		Pattern pattern_p1 = Pattern.compile(regex_p1);
		Matcher matcher2 = pattern_p1.matcher(line);
		if (matcher2.matches()) {
			p1 = matcher2.group(1);

		}
				
		result[0] = interfaceName;
		result[1] = p1;
		result[2] = newsId;
		result[3] = time;
		
		return result;
	}
	
	public static void main (String[] args) {
		
		String inputLine = "10.13.81.241 - - [07/Dec/2011:00:31:24 +0800] GET /api/comment/save.go?newsId=1083922&content=%E8%97%8F%E6%AF%92%E5%B0%B1%E4%B8%80%E5%AE%9A%E8%A6%81%E5%88%A4%E6%AD%BB%E5%88%91%E5%90%97%EF%BC%9F%E5%A6%82%E6%9E%9C%E4%B8%8D%E6%98%AF%E5%A5%B9%E4%BB%AC%E8%87%AA%E5%B7%B1%E6%84%8F%E6%84%BF%E8%BF%90%E8%97%8F%E7%9A%84+%E8%BF%98%E6%98%AF%E8%A6%81%E5%88%A4%E6%AD%BB%E5%88%91%E5%90%97%EF%BC%9F&p1=NjUyNTcxNg%3D%3D HTTP/1.1 \"200\" 141 \"-\" \"Android Sohu News\" \"10.217.129.128\" \"no-cache\" \"MPAPER_API:10.208.0.3:a=4&b=6525716&c=356952045443035&d=4&e=&f=Android&g=2.2.1&h=320x480&i=GT-S5660&j=2050&m=null&q=null&r=0&s=&t=1\"";
		String[] tmp = ParseLog.parseInterface(inputLine);
		for (String t : tmp) {
			System.out.println(t);
		}

	}
}
