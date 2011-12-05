package lib;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;

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
		String regex_time = ".*\\[(.*)\\].*";
		Pattern pattern_time = Pattern.compile(regex_time);
		Matcher matcher_time = pattern_time.matcher(line);
		String time = "";
		if (matcher_time.matches()) {
			time = matcher_time.group(1);
		}
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
		String regex_time = ".*\\[(.*)\\].*";
		Pattern pattern_time = Pattern.compile(regex_time);
		Matcher matcher_time = pattern_time.matcher(line);
		String time = "";
		if (matcher_time.matches()) {
			time = matcher_time.group(1);
		}
		
		result[0] = newsId;
		result[1] = p1;
		result[2] = time;
		
		return result;
	}
}
