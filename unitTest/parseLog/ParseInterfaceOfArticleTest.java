package unitTest.parseLog;

import static org.junit.Assert.assertEquals;
import lib.ParseLog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParseInterfaceOfArticleTest {
	
	private String firstColumn = "";
	private String[] expectedArray = null;
	
	@Before
	public void setUp() {
		firstColumn = "10.13.81.242 - - [25/Nov/2011:00:00:00 +0800] GET /api/news/article.go?newsId=1020147&termId=5417&p1=NTIyMDQ1MQ%3D%3D HTTP/1.1";
		expectedArray = new String[]{"1020147","5220451","2011-11-25:12:00:25"};
	}
	
	@After
	public void tearDown() {
		expectedArray = null;
	}
	
	@Test
	public void test() {
		String[] result = ParseLog.parseInterfaceOfArticle(firstColumn);
		assertEquals(result.length,expectedArray.length);
		for (int i=0; i<result.length; i++)
			assertEquals(result[i],expectedArray[i]);
	}
		
}
