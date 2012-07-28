/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * If the fieldName does contain a ., then it will output the FIRST matching
 * field, if the field appears multiple times or in a child node.
 *
 * If the field contains a ., then it is considered a path, and it must
 * be in the specified location (ex: logdata.ip )
 *
 * You can specify the root JSON node by prefixing the field name with a .
 *
 * If the field is a 0, then the first element of an array is used for that
 * portion of the path (ex: employeeList.0.name ). Other indexes are
 * currently not supported.
 *

Example with search style field names:

REGISTER pig-udf.jar;
DEFINE JsonToTuple com.meta.pigudf.JsonToTuple();

a = load 'moo' as (l:chararray);
b = foreach a generate JsonToTuple($0,'date','ip','ua');
dump b;
((date, ip, ua))

Example with explicit field paths:
b = foreach a generate JsonToTuple($0,'.date','client.ip','client.ua');

Example with flattening the tuple:
b = foreach a generate
    flatten(JsonToTuple($0,'.date','customer.0.name','client.0.age'))
    as (date, name, age);

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
            JsonNode rootNode = mapper.readTree(line);

            for (int i=1; i < input.size(); i++) {
                String field = (String) input.get(i);
				JsonNode currentNode = rootNode;
				if (field.contains(".")) {
					for (String f : field.split("\\.")) {
						// allow ".rootField" paths to explicitly fetch from
						// root and not search entire tree for field name
						if (!f.isEmpty()) {
							// TODO, replace this 0 hack with a fancier array
							// index path follower. But for now we alwyas want
							// the first element anyways.
							if (f == "0") currentNode = currentNode.path(0);
							else currentNode = currentNode.path(f);

							if (currentNode.isMissingNode()) break;
						}
					}
				} else {
					// if no . in path, search entire tree for first match
					currentNode = rootNode.findPath(field);
				}

                if (!currentNode.isMissingNode()) {
                    output.set(i-1,currentNode.getValueAsText());
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