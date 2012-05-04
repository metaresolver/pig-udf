package com.meta.pigudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonParser;

/**
 * Parse a JSON string and output a Tuple.
 * Arguments: jsonString, fieldName1 ... fieldNameN
 * Output: Tuple(fieldValue1 ... fieldValueN)
 *
 * Note: Will output the FIRST matching field, if the field name appears
 * multiple times.
 *
 * Example:

REGISTER pig-udf.jar;
DEFINE JsonToTuple com.meta.pig.JsonToTuple();

a = load 'moo' as (l:chararray);
b = foreach a generate JsonToTuple($0,'date','ip','ua');
dump b;
((date, ip, ua))

 */
@SuppressWarnings("rawtypes")
public class JsonToTuple extends EvalFunc<Tuple> {
    private final TupleFactory tupleFactory = TupleFactory.getInstance();
    private final ObjectMapper mapper = new ObjectMapper()
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    @Override public Tuple exec(Tuple input) throws IOException {
        Tuple output = tupleFactory.newTuple(input.size() - 1);

        // check arguments
        if (input == null || input.size() < 2) {
            throw new IOException("Not enough arguments to JsonToTuple. Got " + input.size() + ", expected at least 2 (json: chararray, fieldName...)");
        }
        // output null if input argument is null
        if (input.get(0) == null) {
            warn("Skipped null input.",PigWarning.UDF_WARNING_1);
            return output;
        }
        // input argument is a string
        if (! (input.get(0) instanceof String)) {
            throw new IOException("Non-String input to JsonToTuple, try casting it to a chararray");
        }
        // assert field names are not null and strings
        for (int i=1; i < input.size(); i++) {
            if (input.isNull(i) || (! (input.get(i) instanceof String))) {
                throw new IOException("Null or non-string fieldName (argument #"+ i +") to JsonToTuple");
            }
        }

        try {
            String line = (String) input.get(0);
            JsonNode node = mapper.readTree(line);

            for (int i=1; i < input.size(); i++) {
                String field = (String) input.get(i);
                JsonNode value = node.findValue(field);
                if (value != null) {
                    output.set(i-1,value.getValueAsText());
                }
            }
        } catch (JsonProcessingException e) {
            warn("Could not parse JSON",PigWarning.UDF_WARNING_3);
        }

        return output;
    }

    @Override public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(input.getField(0).alias,
                                                     DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}