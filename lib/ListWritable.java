package lib;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * A Writable for list containing instances of a class. The elements of this
 * writable must all be instances of the same class. If this writable will be
 * the input for a Reducer, you will need to create a subclass that sets the
 * value to be of the proper type.
 * 
 * For example: <code>
 * public class IntArrayWritable extends ArrayWritable {
 *   public IntArrayWritable() { 
 *     super(IntWritable.class); 
 *   }	
 * }
 * </code>
 */
public class ListWritable implements Writable {
	private Class<? extends Writable> valueClass;
	private List<Writable> values;

	public ListWritable(Class<? extends Writable> valueClass) {
		if (valueClass == null) {
			throw new IllegalArgumentException("null valueClass");
		}
		this.valueClass = valueClass;
	}

	public ListWritable(Class<? extends Writable> valueClass,
			List<Writable> values) {
		this(valueClass);
		this.values = values;
	}

	public Class<? extends Writable> getValueClass() {
		return valueClass;
	}

	public List<Writable> getList() {
		return values;
	}

	public String[] toStrings() {
		String[] strings = new String[values.size()];
		for (int i = 0; i < values.size(); i++) {
			strings[i] = values.get(i).toString();
		}
		return strings;
	}

	public void set(List<? extends Writable> values) {
		this.values = (List<Writable>) values;
	}

	public List<Writable> get() {
		return values;
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		values = new ArrayList<Writable>(size); // construct values
		for (int i=0; i<size; i++) {
			values.add(i, WritableFactories.newInstance(valueClass));
			values.get(i).readFields(in);
		}


	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(values.size()); // write values
		for (Writable value : values) {
			value.write(out);
		}

	}

}
