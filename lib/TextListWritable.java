package lib;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextListWritable extends ListWritable{

	public TextListWritable() {
		super(Text.class);
	}
	
	public TextListWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
		// TODO Auto-generated constructor stub
	}

}
