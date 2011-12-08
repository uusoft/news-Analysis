package unitTest.parseLog;

import static org.junit.Assert.assertEquals;
import lib.ParseLog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParseInterfaceTest2 {

	private String firstColumn = "";
	private String[] expectedArray = null;
	
	@Before
	public void setUp() {
		firstColumn = "10.13.81.242 - - [05/Dec/2011:00:00:03 +0800] GET /api/set.go?m=pubDetail&pubId=1&cid=6942019&p=1&a=a_1&backUrl=set.go%3Fm%3Dsubscribe%26p%3D1%26cid%3D6942019%26t%3D0%26backUrl%3D&p1=Njk0MjAxOQ%3D%3D&p2=QTEwMDAwMjBERDIzQzg%3D&u=1 HTTP/1.1 ";
		expectedArray = new String[]{"/api/set.go","6942019","","2011-12-05:12:00:05"};
	}
	
	@After
	public void tearDown() {
		expectedArray = null;
	}
	
	@Test
	public void test() {
		String[] result = ParseLog.parseInterface(firstColumn);
		assertEquals(result.length,expectedArray.length);
		for (int i=0; i<result.length; i++)
			assertEquals(result[i],expectedArray[i]);
	}
}
