package lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author cuitao
 *���л���ʽ:
 *int	map.length
 *Text	nid
 *Text,DoubleWritable pairs values
 */
public class ItemCooccurrence implements Writable{

	private Text nid;
	private Map<Text,DoubleWritable> cooccurrence;
		
	public ItemCooccurrence() {
		nid = new Text("0");
		cooccurrence = new HashMap<Text,DoubleWritable>(); 
	}
	public ItemCooccurrence(Text nid, Map<Text,DoubleWritable> cooccurrence) {
		set(nid,cooccurrence);
	}
	
	public void set(Text nid, Map<Text,DoubleWritable> cooccurrence) {
		this.nid = nid;
		this.cooccurrence = cooccurrence;
	}
	
	public Text getId() {
		return nid;
	}
	
	public int size() {
		return cooccurrence.size();
	}
	
	public Map<Text,DoubleWritable> getCooccurrence() {
		return cooccurrence;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int mapLength = in.readInt();
		nid.readFields(in);
		cooccurrence = new HashMap<Text,DoubleWritable>(mapLength);
		for (Map.Entry<Text, DoubleWritable> entry : cooccurrence.entrySet()) {
			entry.getKey().readFields(in);
			entry.getValue().readFields(in);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(cooccurrence.size());
		nid.write(out);
		for (Map.Entry<Text, DoubleWritable> entry : cooccurrence.entrySet()) {
			Text itemId = entry.getKey();
			DoubleWritable prefs = entry.getValue();
			
			itemId.write(out);
			prefs.write(out);
		}
		
	}
	
	@Override
	public String toString() {
		
		String result = "";
		for (Map.Entry<Text, DoubleWritable> entry : cooccurrence.entrySet()) {
			Text uid = entry.getKey();
			DoubleWritable importance = entry.getValue();
			result = uid.toString() + "->" + importance.get() + ",";
		}

		return "";
	}

}
