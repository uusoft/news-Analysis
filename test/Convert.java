package test;

import org.apache.hadoop.io.Text;

import lib.ListWritable;

public class Convert {

	public class TextListWritable extends ListWritable {
		public TextListWritable() {
			super(Text.class);
		}
	}
	
	
}
