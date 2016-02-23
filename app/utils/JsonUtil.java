package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class JsonUtil {
    public static JsonNode toJsonNode (String...values) {
        ObjectMapper mapper = new ObjectMapper ();
        ArrayNode array = mapper.createArrayNode();
        if (values != null) {
            for (String v : values) 
                array.add(v);
        }
        return array;
    }
}
