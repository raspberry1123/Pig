package org.kou.pig.func;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class SampleFilter extends FilterFunc {

	@Override
	public Boolean exec(Tuple arg0) throws IOException {
		
		int argLen = arg0.size();
		if(argLen < 2){
			throw new RuntimeException("Argument must be two");
		}
		
		String record = (String)arg0.get(0);
		
		Tuple tuple = (Tuple)arg0.get(1);
		
		int tupleSize = tuple.size();
		for(int i = 0;i < tupleSize;i++){
			if(record.equals((String)tuple.get(i))){
				return true;
			}
		}
		return false;
	}

}
