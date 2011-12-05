package parsePicClickLog;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ParseMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) {

		String firstColumn = value.toString().split("\"")[0];
		String[] temp = parse(firstColumn);
		if (temp == null) {
			return;
		}
		String picId = temp[0];
		String uid = temp[1];
		
		try {
			context.write(new Text(uid), new Text(picId));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**通过nginxlog第一列，解析picId和uid
	 * @param line
	 * @return String数组,列数为2，{picId,uid}
	 */
	private String[] parse (String line) {
		
		//匹配
		String regex_img8 = ".*/img7/.*/([\\d_]*\\.[\\w]*\\?p1=[\\d\\w%]*)(.*)";
//		String regex_img8 = ".*/img7/.*/([0-9]*\\.[a-zA-Z]*\\?p1=[0-9a-zA-Z%]*).*";
		Pattern pattern_img8 = Pattern.compile(regex_img8);
		Matcher matcher = pattern_img8.matcher(line);
		if (matcher.matches()) {
			
			String[] result = new String[2];
			
			String a = matcher.group(1);
			String[] tmp1 = a.split("\\?");
			String picId = tmp1[0].split("_")[0];
			result[0] = picId;
			
			if (tmp1.length !=2) {
				result[1] = "0";
				return result;
			}
			String p1 = tmp1[1].replaceAll("p1=", "");
			try {
				p1 = new String(Base64.decodeBase64(URLDecoder.decode(
						p1, "utf-8").getBytes("utf-8")));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			System.out.println(p1+"\t"+picId);
			result[1] = picId;
			
			return result;
			
		}
		else {
			return null;
		}

	}
	
	public static void main (String[] args) {
//		String testLine = " GET /img7/adapt/wb/2011/12/01/1322722627700_460_1000.jpg?p1=Njc0MjQ2NQ%3D%3D HTTP/1.1 ";
		String testLine = "111.165.146.76 - - [30/Nov/2011:23:59:23 +0800] GET /img7/adapt/wb/2011/10/28/1319765639601_460_1000.png?p1=MzMxMzkxNw%3D%3D HTTP/1.1 ";
		new ParseMapper().parse(testLine);
	}
}
