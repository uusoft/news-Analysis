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
		firstColumn = "10.13.81.242 - - [05/Dec/2011:00:00:03 +0800] GET /api/news/count.go?newsId=1066246 HTTP/1.1 \"200\" 115 \"-\" \"sohunews/2.0.1 CFNetwork/485.12.7 Darwin/10.4.0\" \"118.80.202.155\" \"no-cache\" \"MPAPER_API:118.80.202.155:a=4&b=7660067&c=8804248480c7db88e3044e14931c7bcfee4d73df&d=4&e=&f=iPhone OS&g=4.2.1&h=640x960&i=iPhone&j=1006&m=null&q=null&r=0&s=&t=1";
		expectedArray = new String[]{"/api/news/count.go","7660067","1066246","2011-12-05:12:00:05"};
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
