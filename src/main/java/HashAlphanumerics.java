package com.meta.pigudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Returns a hash of the input, non-empty argument as a CHARARRAY.
Strips non-alphanumeric characters and converts to lowercase before hashing,
so "AB-CD-12-34 " and "abcd1234" have the same hash.

REGISTER pig-udf.jar
DEFINE HashAlphanuerics com.meta.pigudf.HashAlphanumerics();

a = load 'moo' as (id);
b = foreach a generate
    HashAlphanumerics(id)) as hashed_id;

 */
public class HashAlphanumerics extends EvalFunc<Integer> {

    @Override public Integer exec(Tuple input) throws IOException {
        if (input == null)
            return null;

        Object o = input.get(0);
        if (o != null) {
            byte t = input.getType(0);
            String s = null;
            if (DataType.CHARARRAY == t) {
                s = (String) o;
            } else {
                s = DataType.toString(o,t);
            }

            if (! s.isEmpty()) {
                return Integer.valueOf(s.toLowerCase().replaceAll("[^a-z0-9]","").hashCode());
            } else {
                return Integer.valueOf(0);
            }
        }
        return null;
    }

    @Override public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(input.getFields().get(0).alias, DataType.INTEGER));
    }
}