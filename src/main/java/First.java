package com.meta.pigudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Returns the first non-null, non-empty argument as a CHARARRAY.
REGISTER pig-udf.jar
DEFINE First com.meta.pigudf.First();

a = load 'moo' as (homephone, cellphone, workphone);
b = foreach a generate
    First(homephone, cellphone, workphone) as phone;

 */
public class First extends EvalFunc<String> {

    @Override public String exec(Tuple input) throws IOException {
        if (input == null) 
            return null;

        for (int i = 0; i < input.size(); i++) {
            Object o = input.get(i);
            if (o != null) {
                byte t = input.getType(i);
                String s = null;
                if (DataType.CHARARRAY == t) {
                    s = (String) o;
                } else {
                    s = DataType.toString(o,t);
                }
                if (! s.isEmpty()) {
                    return s;
                }
            }
        }
        return null;
    }

    @Override public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(input.getFields().get(0).alias, DataType.CHARARRAY));
    }
}