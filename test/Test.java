package test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Test {

	public static void main (String[] args) {
//		List<Text> a =new ArrayList<Text>();
//		a.add(new Text("a"));
//		a.add(new Text("b"));
//		a.add(new Text("c"));
//		a.add(new Text("d"));
//		
//		Iterable<Text> nidList = a;
//		ArrayList<Text> arrayList = new ArrayList<Text>();
//		for (Text t : nidList) {
//			arrayList.add(t);
//		}
//
//		Text[] array = new Text[arrayList.size()];
//		int i=0;
//		for (Text t : nidList) {
//			array[i] = t;
//			i++;
//		}
//		
		ArrayList<Text> aa = new ArrayList<Text>(4);
		aa.set(1, new Text("a"));
		
		for (Text t : aa) {
			System.out.println(t.toString());
		}
		
		
	}
}
