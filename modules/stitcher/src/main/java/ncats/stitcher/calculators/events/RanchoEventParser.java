package ncats.stitcher.calculators.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ncats.stitcher.calculators.EventCalculator;

import java.lang.reflect.Array;
import java.util.*;
import java.util.logging.Level;

public class RanchoEventParser extends EventParser {
    final ObjectMapper mapper = new ObjectMapper ();
    final Base64.Decoder decoder = Base64.getDecoder();
    Event event;
    Object id;

    public RanchoEventParser() {
        super ("rancho-export_2017-10-26_20-39.json");
    }

    void parseEvent (JsonNode n) {
        if (n.has("HighestPhase") && ("approved".equalsIgnoreCase
            (n.get("HighestPhase").asText())) || ("phase IV".equalsIgnoreCase
                (n.get("HighestPhase").asText()))) {
            event = new Event(name, id, Event.EventKind.Marketed);
            if (n.has("HighestPhaseUri")) {
                event.comment = n.get("HighestPhaseUri").asText();
                if (event.comment.contains("fda.gov")) {
                    event.jurisdiction = "US";
                }
            }
            else {
                event.comment = "";
                if (n.has("ConditionName"))
                    event.comment +=
                            n.get("ConditionName").asText();
                else if (n.has("ConditionMeshValue"))
                    event.comment +=
                            n.get("ConditionMeshValue").asText();
                if (n.has("ConditionProductName"))
                    event.comment += " "
                        + n.get("ConditionProductName").asText();
            }
            if (n.has("ConditionProductDate")) {
                String d = n.get("ConditionProductDate").asText();
                try {
                    Date date = EventCalculator.SDF.parse(d);
                        event.date = date;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    EventCalculator.logger.log(Level.SEVERE,
                            "Can't parse date: "+d, ex);
                }
            }

            if(n.has("InVivoUseRoute")){
                String route = n.get("InVivoUseRoute").asText();
                //InVivoUseRoute is a controlled vocabularly
                //if the value = "other" than use the field InVivoUseRouteOther
                //which is free text.
                if("other".equalsIgnoreCase(route)){
                    route = n.get("InVivoUseRouteOther").asText();
                }
                if(route !=null && !route.trim().isEmpty()){
                    event.route = route;
                }
            }

        }
    }

    void parseEvent (String content) throws Exception {
        JsonNode node = mapper.readTree(decoder.decode(content));
        if (node.isArray()) {
            for (int i = 0; i < node.size(); ++i) {
                JsonNode n = node.get(i);
                parseEvent (n);
            }
        }
        else
            parseEvent (node);
    }

    public List<Event> getEvents(Map<String, Object> payload) {
        List<Event> events = new ArrayList<>();
        event = null;
        id = payload.get("Unii");
        if (id != null && id.getClass().isArray()
            && Array.getLength(id) == 1)
            id = Array.get(id, 0);

        Object content = payload.get("Conditions");
        if (content != null) {
            if (content.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(content); ++i) {
                    String c = (String) Array.get(content, i);
                    try {
                        parseEvent (c);
                    }
                    catch (Exception ex) {
                        EventCalculator.logger.log(Level.SEVERE,
                                   "Can't parse Json payload: "+c, ex);
                    }
                }
            }
            else {
                try {
                    parseEvent ((String)content);
                }
                catch (Exception ex) {
                    EventCalculator.logger.log(Level.SEVERE,
                               "Can't parse json payload: "+content, ex);
                }
            }
        }
        if (event != null) events.add(event);
        return events;
    }
}
