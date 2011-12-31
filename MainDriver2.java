import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import test.TestMapWritableJob;

import lib.ToolJob;


public class MainDriver2 {
	
	private Map<String,Class<?>> classMap = new HashMap<String,Class<?>>();
	
	public MainDriver2() {
		add("parseInterface.Driver", parse.api.Driver.class);
		add("parseClickLog.Driver", parse.clickLog.Driver.class);
		add("sequenceFileTest.Driver", sequenceFileTest.Driver.class);
		add("parseReceive.Driver", parse.receive.Driver.class);
		add("parseClickLogV2.Driver", parse.clickLogV2.Driver.class);
		add("parseClickLog.Driver", parse.clickLog.Driver.class);
		add("recommend", itemBaseRecommendation2.Driver.class);
		add("tempAnalysis.Driver", tempAnalysis.Driver.class);
		add("parsePicClickLog.Driver", parse.picClickLog.Driver.class);
		add("api-stat", stat.api.UvDriver.class);
		add("text2Seq", parse.FileParseTextToSequence.class);
		add("test", TestMapWritableJob.class);
		add("testIO", test.FileOperation.class);
		add("lastvisit", parse.lastvisit.Driver.class);
		add("pref", pref.PrefJob.class);
	}
	
	public static void main (String[] args) throws Exception {
		MainDriver2 driver = new MainDriver2();
		String className = args[0];
		if (args[0].equalsIgnoreCase("-help")) {
			driver.printHelp();
			System.exit(0);
		}
		if (!driver.find(className)) {
			System.out.println("do not have this class");
			System.exit(0);
		}
		else {
			String[] newArgs = new String[args.length-1];
			for (int i=1; i<args.length; i++) {
				newArgs[i-1] = args[i];
			}
			ToolRunner.run((Tool) driver.getClass(className).newInstance(), newArgs);

		}
	}
	public void add(String className,Class<?> cls) {
		classMap.put(className, cls);
		
	}
	
	public Class<?> getClass(String className) {
		return classMap.get(className);
	}
	public void printHelp() {
		for (Map.Entry<String, Class<?>> entry: classMap.entrySet()) {
			System.out.println(entry.getKey());
		}
			
	}
	
	public boolean find (String key) {
		if (classMap.containsKey(key))
			return true;
		else
			return false;
	}
	
	

}
