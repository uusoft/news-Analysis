package unitTest.parseLog;

import static org.junit.Assert.assertEquals;
import lib.ParseLog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParseInterfaceTest {

	private String firstColumn = "";
	private String[] expectedArray = null;
	
	@Before
	public void setUp() {
		firstColumn = "10.13.81.242 - - [05/Dec/2011:00:00:03 +0800] GET /api/welcome.do?p1=NzI1MDgyMA%3D%3D&p2=QTEwMDAwMjBGMDM1RjM%3D&u=1 HTTP/1.1 \"302\" 124 \"-\" \"Mozilla/5.0 (Linux; U; Android 2.3.4; zh-cn; ZTE-C_N760/N760V1.0.0B03; 320*480; CTC/2.0) AppleWebkit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1\" \"183.40.144.78\" \"must-revalidate; no-cache; no-store\" \"MPAPER_API:183.40.144.78:a=3&b=7250820&c=A1000020F035F3&d=3&e=&f=Android&g=2.3.4&h=320x480&i=ZTE-C N760&j=0&m=null&q=null&r=0&s=&t=1\"";
		expectedArray = new String[]{"/api/welcome.do","7250820","","2011-12-05:12:00:05"};
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
