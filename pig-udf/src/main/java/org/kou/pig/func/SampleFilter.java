package org.kou.pig.func;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class SampleFilter extends FilterFunc {

	@Override
	public Boolean exec(Tuple arg0) throws IOException {
		
		int argLen = arg0.size();
		if(argLen < 2){
			throw new RuntimeException("Argument must be two");
		}
		
		String record = (String)arg0.get(0);
		
		DataBag bag = (DataBag)arg0.get(1);
		
		Iterator<Tuple> iterator = bag.iterator();
		while(iterator.hasNext()){
			Tuple tuple =  iterator.next();
			String checkerField = (String)tuple.get(1);
			if(record.equals(checkerField)){
				return true;
			}
		}
		return false;
	}

}
