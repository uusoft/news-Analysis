package parse.receive;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Utils {

	public static Pattern patternCheckB = Pattern.compile("b=(\\d*)");
	public static Pattern patternCheckT = Pattern.compile("t=([\\d-_,]*)");
	public static Pattern patternG = Pattern.compile("m102=([\\d-_,]*)");
	public static Pattern patternMPaperLine = Pattern.compile("b=(\\d*)");
	public static Pattern patternMPaperLineF = Pattern.compile("f=(\\b\\w*\\b)");
	
	/**
	 * @param line	����ֵ��һ��nginx log
	 * @return	String���� ��userId,ƽ̨��ƣ�termId(�Զ��ŷָ�)��
	 */
	public static List<String> parseUrl(String line) {
		List<String> result = new ArrayList<String>();

		String[] temp3 = line.split("\"");
		if (temp3.length < 12) {
//			for (String s : temp3) {
//				System.out.println(s);
//			}
//			System.out.println("-----------");
			return null;
		}
		String url = temp3[0];
		String mpaperLine = temp3[11];
		
		
		
		//ƥ��userId
		Matcher matcherB = patternMPaperLine.matcher(mpaperLine);
		if (matcherB.find()) {
//			System.out.println(matcherB.group());
//			System.out.println(matcherB.group(1));
			result.add(matcherB.group(1));
		}
		else {
			return null;
		}
		
		Matcher matcherC  = patternMPaperLineF.matcher(mpaperLine);
		if (matcherC.find())
			result.add(matcherC.group(1));
		
		if (url.contains("check.do")) {
			
			Matcher matcherT = patternCheckT.matcher(url);
			if (matcherT.find()) {
//				System.out.println(matcherT.group());
//				System.out.println(matcherT.group(1));
				String temp1 = matcherT.group(1);
				String[] ts = temp1.split("_");
				if (ts.length != 2)
					return null;
				String[] t2s = ts[1].split(",");
				for (String a : t2s) {
//					System.out.println(a);
					result.add(a);
				}
			}
		}
		else if (url.contains("g.do")) {
			
			Matcher matcherT = patternG.matcher(url);
			if (matcherT.find()) {
//				System.out.println(matcherT.group());
//				System.out.println(matcherT.group(1));
				String temp1 = matcherT.group(1);
				String[] ts = temp1.split("_");
				if (ts.length != 2)
					return null;
				String[] t2s = ts[1].split(",");
				for (String a : t2s) {
//					System.out.println(a);
					result.add(a);
				}
			}
			
			
			
		}
		

		return result;
	}
	
	
	public static void main (String[] args) {
		
		List<String> lines = ReadFromFile.readFileByLines("test.log");
		System.out.println(lines.get(0));
		List<String> aa = parseUrl(lines.get(0));
		for (String a : aa) {
			System.out.println(a);
		}

	}
}
