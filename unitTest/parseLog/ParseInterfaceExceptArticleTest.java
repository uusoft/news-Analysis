package unitTest.parseLog;

import static org.junit.Assert.assertEquals;
import lib.ParseLog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParseInterfaceExceptArticleTest {

	private String firstColumn = "";
	private String[] expectedArray = null;
	
	@Before
	public void setUp() {
		firstColumn = "10.13.81.241 - - [09/Nov/2011:23:59:01 +0800] GET /api/news/count.go?newsId=902882&p1=MjE1OTU2 HTTP/1.1";
		expectedArray = new String[]{"/api/news/count.go","215956","2011-11-09:11:59:09"};
	}
	
	@After
	public void tearDown() {
		expectedArray = null;
	}
	
	@Test
	public void test() {
		String[] result = ParseLog.parseInterfaceExceptArticle(firstColumn);
		assertEquals(result.length,expectedArray.length);
		for (int i=0; i<result.length; i++)
			assertEquals(result[i],expectedArray[i]);
	}
}
