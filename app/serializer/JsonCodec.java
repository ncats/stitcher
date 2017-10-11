package serializer;

import com.fasterxml.jackson.databind.JsonNode;
import ncats.stitcher.CNode;

public interface JsonCodec {
    JsonNode encode (CNode node);
}
