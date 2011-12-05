package unitTest.parseLog;

import static org.junit.Assert.assertEquals;
import lib.ParseLog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParseHtmlLogTest {
	
	private String firstColumn = "";
	private String[] expectedArray = null;
	
	@Before
	public void setUp() {
		firstColumn = "49.112.31.250 - - [22/Nov/2011:00:03:14 +0800] GET /mpaper/2/20111121/5339_2_0_2_0_0_0_74/mpnws_6_2_1001038_p1.html?p1=ODU5NzAxNg%3D%3D&p2=QTAwMDAwMzIzRUQzRkY%3D&u=1 ";
		expectedArray = new String[]{"1001038","8597016","22/Nov/2011:00:03:14 +0800"};
	}
	
	@After
	public void tearDown() {
		expectedArray = null;
	}
	
	@Test
	public void test() {
		String[] result = ParseLog.parseHtmlLog(firstColumn);
		assertEquals(result.length,expectedArray.length);
		for (int i=0; i<result.length; i++)
			assertEquals(result[i],expectedArray[i]);
	}
	
	public static void main (String[] args) {
		String line = "49.112.31.250 - - [22/Nov/2011:00:03:14 +0800] GET /mpaper/2/20111121/5339_2_0_2_0_0_0_74/mpnws_6_2_1001038_p1.html?p1=ODU5NzAxNg%3D%3D&p2=QTAwMDAwMzIzRUQzRkY%3D&u=1 "; 
		String[] r = ParseLog.parseHtmlLog(line);
		for (String s : r) {
			System.out.println(s);
		}

	}
		

}
