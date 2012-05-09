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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ValueNode;
import org.codehaus.jackson.JsonParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Parses JSON data from a JSON file one line at a time. Each line is expected
 * to contain a separate JSON record.
 *
 * Usage:
 * e.g. JSON that looks like:
 * --------------------------------------------------------------
 * {"menu": {
 *    "id": "file",
 *    "value": "File",
 *    "popup": {
 *      "menuitem": [
 *        {"value": "New", "onclick": "CreateNewDoc()"},
 *        {"value": "Open", "onclick": "OpenDoc()"},
 *        {"value": "Close", "onclick": "CloseDoc()"}
 *      ]
 *    }
 * }}
 * --------------------------------------------------------------
 * **The above json record is expanded for readability. This entire record should be
 * condensed to one line in your json file.
 *
 *  register the jar containing this class (e.g. piggybank.jar)
 *  a = load '/tmp/jsontest' using org.pig.piggybank.storage.JsonLoader() as (json:map[]);
 *  b = foreach a generate flatten(json#'menu') as menu;
 *  c = foreach b generate flatten(menu#'popup') as popup;
 *  d = foreach c generate flatten(popup#'menuitem') as menu;
 *  e = foreach d generate flatten(men#'value') as val;
 *
 */
public class JsonLoader extends LoadFunc {
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private ObjectMapper mapper;
    private LineRecordReader in = null;

    public JsonLoader() {
        super();
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();
    }

    /**
     * Only return null if there is no more input in this file!
     */
    @Override
    public Tuple getNext() throws IOException {
	if (in.nextKeyValue()) {
	    Text val = in.getCurrentValue();
	    if (val != null) {
		return parseStringToTuple(val.toString());
	    }
	}
	return null;
    }

    protected Tuple parseStringToTuple(String line) throws IOException {
	if (null == line || line.length() == 0 || line.charAt(0) != '{') {
	    return tupleFactory.newTuple(0);
	}

        try {
            return tupleFactory.newTuple(walkObject(mapper.readTree(line)));
        } catch (NumberFormatException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input - Very big number exceeds the scale of long: " + line;
            throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
        } catch (JsonParseException e) {
            return tupleFactory.newTuple(0); // JSON parsing errors yield empty tuple
        }
    }

    /**
     * Walks the given JsonObject,
     */
    private Map<String,Object> walkObject(JsonNode node) {
	Map<String,Object> values = new HashMap<String, Object>();

        Iterator<String> keys = node.getFieldNames();
        Iterator<JsonNode> nodes = node.getElements();
        while (keys.hasNext()) {
            String key = keys.next();
            JsonNode value = nodes.next();

            if (value.isArray()) {
		values.put(key, walkArray(value));
            } else if (value.isObject()) {
		values.put(key,tupleFactory.newTuple(walkObject(value)));
            } else if (value.isNull()) {
		values.put(key, null);
	    } else if (value.isValueNode()) {
		values.put(key, value.getValueAsText());
            }
        }
	return values;
    }

    /**
     * Walks the given JsonArray, adding contents to the given DataBag
     */
    private DataBag walkArray(JsonNode arr) {
	DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
	for (JsonNode value : arr) {
	    if (value.isArray()) {
		bag.addAll(walkArray(value));
	    } else if (value.isObject()){
		bag.add(tupleFactory.newTuple(walkObject(value)));
	    } else if (value.isValueNode() && ! value.isNull()) {
		String s = value.getValueAsText();
		if (s != null) { // mot sure this is necessary, just conservative
		    bag.add( tupleFactory.newTuple(s) );
		}
	    }
	}
	return bag;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        in = (LineRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        PigFileInputFormat.setInputPaths(job, location);
    }
}