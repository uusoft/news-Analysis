package single;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Test {

	public static void main (String[] args) throws IOException {
//		String inputFile = args[0];
		String inputFile = "/home/cuitao/workspace/temp.log";
		File file = new File(inputFile);
		FileReader fr = new FileReader(file);
		BufferedReader input = new BufferedReader(fr);
		String line = null;
		input.readLine();
		
		Map<String,Set<String>> map = new HashMap<String,Set<String>>();
		while ((line = input.readLine() ) != null) {
//			System.out.println(line);
			String tmp1[] = line.split("\t");
			String nid = tmp1[0];
			String tmp2[] = tmp1[1].split(",");
			String uid = tmp2[0];
			if (nid == null || nid.length()<4 || uid == null || uid.length()<4) {
				continue;
			}
			if (map.containsKey(uid)) {
				map.get(uid).add(nid);
			}
			else {
				map.put(uid, new HashSet<String>());
			}
		}
		input.close();
		fr.close();
		
//		print(map);
		System.out.println("begin compute");

		//compute similarity
		Map<String,Integer> resultMap = new HashMap<String,Integer>();
		for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
			for (Map.Entry<String, Set<String>> entry2 : map.entrySet()) {
				String key = entry.getKey()+","+entry2.getKey();
				if (entry.getKey().equals(entry2.getKey()) || exist(key,resultMap)) {
					continue;
				}
				int result = computeSimlarity(entry.getValue(),entry2.getValue());
				
				resultMap.put(key,result);
			}
		}
		
		for (Map.Entry<String, Integer> entry : resultMap.entrySet()) {
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}
	
	public static void print(Map<String,Set<String>> map) {
		for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
			System.out.print(entry.getKey()+":");
			for (String nid : entry.getValue()) {
				System.out.print("\t"+nid);
			}
			System.out.println();
		}
	}
	public static boolean exist (String key, Map<String,Integer> map ) {
		String[] tmp = key.split(",");
		String key2 = tmp[1]+","+tmp[0];
		if (map.size() == 0)
			return false;
		if (map.containsKey(key) || map.containsKey(key2)) {
			return true;
		}
		else 
			return false;
	}
	public static int computeSimlarity(Set<String> set1, Set<String> set2) {
		int result = 0;
		for (String s : set1) {
			if (set2.contains(s)) {
				result++;
			}
		}
		
		return result;
	}
	
	
}










