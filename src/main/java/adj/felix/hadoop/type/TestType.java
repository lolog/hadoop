package adj.felix.hadoop.type;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestType {
	@Test
	public void string () throws UnsupportedEncodingException {
		String str = "\u0041\u00DF\u6771\uD801\uDC00";
		System.out.println(str.length());
		System.out.println(str.getBytes("UTF-8").length);
		
		System.out.println(str.indexOf("\u0041"));
		System.out.println(str.indexOf("\u00DF"));
		System.out.println(str.indexOf("\u6771"));
		System.out.println(str.indexOf("\uD801"));
		
		System.out.println(str.charAt(0) == '\u0041');
		System.out.println(str.charAt(1) == '\u00DF');
		System.out.println(str.charAt(2) == '\u6771');
		System.out.println(str.charAt(3) == '\uD801');
		
		System.out.println(str.codePointAt(0) == 0x0041);
		System.out.println(str.codePointAt(1) == 0x00df);
		System.out.println(str.codePointAt(2) == 0x6771);
		System.out.println(str.codePointAt(3) == 0x10400);
	}
	
	@Test
	public void text() {
		Text text = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		System.out.println(text.getLength());
		System.out.println(text.find("\u0041"));
		System.out.println(text.find("\u00DF"));
		System.out.println(text.find("\u6771"));
		System.out.println(text.find("\uD801\uDC00"));
		
		System.out.println(text.charAt(0) == 0x0041);
		System.out.println(text.charAt(1) == 0x00df);
		System.out.println(text.charAt(3) == 0x6771);
		System.out.println(text.charAt(6) == 0x10400);
		System.out.println(text.charAt(10));
	}
	
	@Test
	public void cycle () {
		Text t = new Text("hello word");
		Text text = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0 , text.getLength());
		int cp;
		while (buffer.hasRemaining() && (cp = Text.bytesToCodePoint(buffer)) > -1) {
			System.out.println(Integer.toHexString(cp));
		}
		System.out.println(t);
	}
}
