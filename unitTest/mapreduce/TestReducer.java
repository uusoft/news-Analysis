package unitTest.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import pref.PrefReducer;

public class TestReducer {

	private PrefReducer reducer;

	private ReduceDriver reduceDriver;

	@Before
	public void setUp() {
		reducer = new PrefReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
	}

	@Test
	public void testWCMapper() {
		List<Text> list = new ArrayList<Text>();
		list.add(new Text("a"));
		list.add(new Text("b"));
//		list.add(new Text("c"));

//		reduceDriver.withReducer(reducer).withInput(new Text("1"), list)
//		.withOutput(new Text("a"),new Text("b"))
//		.withOutput(new Text("a"),new Text("c"))
//		.withOutput(new Text("c"),new Text("b"))
//				.runTest();
		reduceDriver.withReducer(reducer).withInput(new Text("1"), list)
		.withOutput(new Text("a"),new Text("b"))
		.withOutput(new Text("b"),new Text("a"))
//		.withOutput(new Text("b"),new Text("c"))
//		.withOutput(new Text("b"),new Text("b"))
				.runTest();

	}
}
