package lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CooccurrenceOrPrefWritable implements Writable{
	
//	private IntWritable cooccurrenceColumnLength;
//	private IntWritable uidPrefMapLength;
	private Text nid;
	private ItemCooccurrence itemCooccurrence;
	private MapWritable userPrefMap;
	
	public CooccurrenceOrPrefWritable() {
		itemCooccurrence = new ItemCooccurrence();
		userPrefMap = new MapWritable();
	}

	public ItemCooccurrence getItemCooccurrence() {
		return itemCooccurrence;
	}
	
	public MapWritable getUserPrefMap() {
		return userPrefMap;
	}
	
	public void setItemCooccurrence(ItemCooccurrence itemCooccurrence) {
		this.itemCooccurrence = itemCooccurrence;
	}
	
	public void setMapWritable (MapWritable mapWritable) {
		this.userPrefMap = mapWritable;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		nid.write(out);
		out.writeInt(itemCooccurrence.size());
		out.writeInt(userPrefMap.size());
		itemCooccurrence.write(out);
		userPrefMap.write(out);
		
		
		
	}
	
	
	

}
