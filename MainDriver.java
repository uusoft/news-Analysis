import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.TestMapWritable;

import test.TestMapWritableJob;

public class MainDriver {

private Map<String,Class<?>> classMap = new HashMap<String,Class<?>>();
	
	public void add(String className,Class<?> cls) {
		classMap.put(className, cls);
		
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

	public void run(String className, String[] args) {
		Class<?>[] parameterTypes = new Class<?>[] {String[].class};
		try {
			classMap.get(className).getMethod("main", parameterTypes).invoke(null, new Object[]{args});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		MainDriver runDriver = new MainDriver();
		runDriver.add("parseInterface.Driver", parse.api.Driver.class);
		runDriver.add("parseClickLog.Driver", parse.clickLog.Driver.class);
		runDriver.add("sequenceFileTest.Driver", sequenceFileTest.Driver.class);
		runDriver.add("parseReceive.Driver", parse.receive.Driver.class);
		runDriver.add("similarity.Driver", similarity.Driver.class);
		runDriver.add("parseClickLogV2.Driver", parse.clickLogV2.Driver.class);
		runDriver.add("parseClickLog.Driver", parse.clickLog.Driver.class);
		runDriver.add("recommend", itemBaseRecommendation2.Driver.class);
		runDriver.add("tempAnalysis.Driver", tempAnalysis.Driver.class);
		runDriver.add("parsePicClickLog.Driver", parse.picClickLog.Driver.class);
		runDriver.add("api-stat", stat.api.UvDriver.class);
		runDriver.add("text2Seq", parse.FileParseTextToSequence.class);
		runDriver.add("test", TestMapWritableJob.class);
		runDriver.add("lastvisit", parse.lastvisit.Driver.class);

		String className = args[0];
		if (args[0].equalsIgnoreCase("-help")) {
			runDriver.printHelp();
			System.exit(0);
		}
		if (!runDriver.find(className)) {
			System.out.println("do not have this class");
			System.exit(0);
		}
		else {
			String[] newArgs = new String[args.length-1];
			for (int i=1; i<args.length; i++) {
				newArgs[i-1] = args[i];
			}
			runDriver.run(className, newArgs);
		}

	}
}
