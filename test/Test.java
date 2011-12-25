package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Test {

	public static class A implements Writable{
		private String a;
		public A (String a) {
			this.a = a;
		}
		
		@Override
		public String toString() {
			return a;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
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
//		ArrayList<Text> aa = new ArrayList<Text>(4);
//		aa.set(1, new Text("a"));
//		
//		for (Text t : aa) {
//			System.out.println(t.toString());
//		}
		
		A[] array = new A[]{new A("a"), new A("b"),new A("c")};
		List<A> list = new ArrayList<A>();
		list.add(new A("a"));
		list.add(new A("b"));
		list.add(new A("c"));
		Map<A,String> map = new HashMap<A,String>();
//		for (A a : list) {
//			map.put(a, "1");
//		}
		List<Text> listT = new ArrayList<Text>();
		listT.add(new Text("a"));
		listT.add(new Text("b"));
		listT.add(new Text("c"));
		Iterable<Text> i = listT;
		
		MapWritable mw = new MapWritable();
		for (Text t : i) {
			mw.put(t, new Text("1"));
		}
		
//		for (Entry<A, String> entry : map.entrySet()) {
//			System.out.println(entry.getKey()+":"+entry.getValue());
//		}
		
		for (Entry<Writable, Writable> entry : mw.entrySet()) {
			System.out.println((Text) entry.getKey()+":"+(Text) entry.getValue());
		}
		
	}
}
