package foo.org;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
	protected KeyComparator(){
		super(CompositeKey.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		CompositeKey c1 = (CompositeKey) a;
		CompositeKey c2 = (CompositeKey) b;
		
		int cmp = c1.compareTo(c2);
		if(cmp != 0){
			return cmp;
		}
		else{
			return -c1.getNaturalValue().compareTo(c2.getNaturalValue());
		}
	}

}
