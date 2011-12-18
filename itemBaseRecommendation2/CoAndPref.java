package itemBaseRecommendation2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CoAndPref implements Writable{
	
	private Text nid;
	private MapWritable co;
	private MapWritable pref;
	private BooleanWritable hasCo = new BooleanWritable(false);
	private BooleanWritable hasPref = new BooleanWritable(false);
	
	public CoAndPref() {
		nid = new Text("");
		co = new MapWritable();
		pref = new MapWritable();
	}
	
	public Text getNid() {
		return nid;
	}

	public void setNid(Text nid) {
		this.nid = nid;
	}

	public MapWritable getCo() {
		return co;
	}
	public void setCo(MapWritable co) {
		this.co = co;
	}
	
	public MapWritable getPref() {
		return pref;
	}
	public void setPref(MapWritable pref) {
		this.pref = pref;
	}

	public BooleanWritable getHasCo() {
		return hasCo;
	}

	public void setHasCo(BooleanWritable hasCo) {
		this.hasCo = hasCo;
	}

	public BooleanWritable getHasPref() {
		return hasPref;
	}

	public void setHasPref(BooleanWritable hasPref) {
		this.hasPref = hasPref;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		nid.write(out);
		out.writeInt(co.size());
		co.write(out);
		out.writeInt(pref.size());
		pref.write(out);
		hasCo.write(out);
		hasPref.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		nid = new Text(Text.readString(in));
		int coSize = in.readInt();
		co.readFields(in);
		int prefSize = in.readInt();
		pref.readFields(in);
		hasCo.readFields(in);
		hasPref.readFields(in);
		
	}

	
	

}
