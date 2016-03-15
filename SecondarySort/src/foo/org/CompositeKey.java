package foo.org;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {
	
	private IntWritable naturalKey;
	private IntWritable naturalValue;
	
	public CompositeKey(){
		set(new IntWritable(),new IntWritable());
	}
	public CompositeKey(int key,int value) {
		// TODO Auto-generated constructor stub
		this.naturalKey = new IntWritable(key);
		this.naturalValue = new IntWritable(value);
	}
	
//	public CompositeKey(IntWritable key,IntWritable value) {
//		// TODO Auto-generated constructor stub
//		this.naturalKey = key;
//		this.naturalValue = value;
//	}
	
	public void set(IntWritable naturalKey,IntWritable naturalValue){
		this.naturalKey = naturalKey;
		this.naturalValue = naturalValue;
	}
	
	


//	@Override
//	public int hashCode() {
//		// TODO Auto-generated method stub		
//		return naturalKey.hashCode(); 
//	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return naturalKey.toString();
	}

	public IntWritable getNaturalKey() {
		return naturalKey;
	}
	public IntWritable getNaturalValue() {
		return naturalValue;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		naturalKey.readFields(in);
		naturalValue.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		naturalKey.write(out);
		naturalValue.write(out);
	}



	@Override
	public  int compareTo(CompositeKey ck) {
		// TODO Auto-generated method stub
		int cmp = naturalKey.compareTo(ck.naturalKey);
		if(cmp != 0){
			return cmp;
		}
		else{
			return naturalValue.compareTo(naturalValue);
		}
		
	}

}
