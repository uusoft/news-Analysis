package lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author cuitao
 *
 *int	map.length
 *Text	nid
 *Text,DoubleWritable pairs values
 */
public class ItemCooccurrence implements Writable{

	private Text nid;
	private MapWritable cooccurrence;
		
	public ItemCooccurrence() {
		nid = new Text("0");
		cooccurrence = new MapWritable(); 
	}
	public ItemCooccurrence(Text nid, MapWritable cooccurrence) {
		set(nid,cooccurrence);
	}
	
	public void set(Text nid, MapWritable cooccurrence) {
		this.nid = nid;
		this.cooccurrence = cooccurrence;
	}
	
	public Text getId() {
		return nid;
	}
	
	public MapWritable getCooccurrence() {
		return cooccurrence;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		nid.readFields(in);
		cooccurrence.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		nid.write(out);
		cooccurrence.write(out);
		
	}
	
	@Override
	public String toString() {
		
		String result = "";
		for (Entry<Writable,Writable> entry : cooccurrence.entrySet()) {
			Text nid = (Text) entry.getKey();
			IntWritable coValue = (IntWritable) entry.getValue();
			result = nid.toString() + "->" + coValue.get() + ",";
		}

		return result;
	}

}
