package com.hortonworks.target.dse.delta;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

public class TupleMerge extends EvalFunc<Tuple> {
	@Override
	public Tuple exec(Tuple group) throws IOException {
		DataBag values = (DataBag)group.get(0);
		Tuple result = null;
		for(Tuple value : values) {
			if (result == null) {
				result = value;
			}

			int fieldIndex = 0;
			for(Object fieldValue : value) {
				if(fieldValue != null) {
					// If it's not a string, set it. If it is, make sure it's not empty
					if(!(fieldValue instanceof String) || !((String)fieldValue).isEmpty()) {
						result.set(fieldIndex, fieldValue);
					}
				}
				fieldIndex++;
			}
		}

		return result;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return input;
	}
}
