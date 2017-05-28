package edu.neu.utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created by paulomimahidharia on 5/27/17.
 */
public class JsonProcessing {

    public static ObjectNode parseStringToJson(String s) throws ParseException, IOException {

        return new ObjectMapper().readValue(s, ObjectNode.class);
    }
}
