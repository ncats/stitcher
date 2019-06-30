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
        super ("Rancho BioSciences, March 2019");
    }

    void parseCondition(JsonNode n) {
        if (n.has("HighestPhase") && n.get("HighestPhase") != null &&
                ("approved".equalsIgnoreCase(n.get("HighestPhase").asText()) ||
                ("phase IV".equalsIgnoreCase(n.get("HighestPhase").asText())))) {
            event = new Event(name, id, Event.EventKind.Marketed);
            if (n.has("HighestPhaseUri")) {
                event.URL = n.get("HighestPhaseUri").asText();
                if (event.URL.contains("fda.gov")) {
                    event.jurisdiction = "US";
                    // While Rancho is manually curated, they shouldn't override annotations directly from FDA
                    // Such annotations need to be manually reviewed
//                    if (event.URL.contains("Veterinary") || event.URL.contains("Animal"))
//                        event.kind = Event.EventKind.USAnimalDrug;
//                    else
//                        event.kind = Event.EventKind.USApprovalRx;
                }
                else if (event.URL.contains("dailymed.nlm.nih.gov")) {
                    event.jurisdiction = "US";
                    // While Rancho is manually curated, they shouldn't override annotations directly from FDA
                    // Such annotations need to be manually reviewed
//                    event.kind = Event.EventKind.USUnapproved; // TODO Update Rancho highest phase to match new nomenclature
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
            if (n.has("ConditionProductName"))
                event.product = n.get("ConditionProductName").asText();
            if (n.has("ConditionProductDate")) {
                String d = n.get("ConditionProductDate").asText();
                if (!"Unknown".equals(d))
                try {
                    Date date = EventCalculator.SDF.parse(d);
                        event.startDate = date;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    EventCalculator.logger.log(Level.SEVERE,
                            "Can't parse startDate: "+d, ex);
                }
            }
        }
    }

    void parseCondition(String content) throws Exception {
        JsonNode node = mapper.readTree(decoder.decode(content));
        if (node.isArray()) {
            for (int i = 0; i < node.size(); ++i) {
                JsonNode n = node.get(i);
                parseCondition(n);
            }
        }
        else
            parseCondition(node);
    }

    String parseObject (Object item) {
        if (item != null) {
            String value = "";
            if (item.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(item); i++)
                    value += (value.length() > 0 ? ";" : "") +
                            Array.get(item, i).toString();
            } else value = item.toString();
            return value;
        }
        return null;
    }

    public void produceEvents(Map<String, Object> payload) {
        event = null;
        id = payload.get("Unii");
        if (id != null && id.getClass().isArray())
            //&& Array.getLength(id) == 1)
            id = Array.get(id, 0); //TODO Make Rancho UNII unique, for now assume first UNII is good surrogate

        // First, find highest phase product from conditions list
        Object content = payload.get("Conditions");
        if (content != null) {
            if (content.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(content); ++i) {
                    String c = (String) Array.get(content, i);
                    try {
                        parseCondition(c);
                    }
                    catch (Exception ex) {
                        EventCalculator.logger.log(Level.SEVERE,
                                   "Can't parse Json payload: "+c, ex);
                    }
                }
            }
            else {
                try {
                    parseCondition((String)content);
                }
                catch (Exception ex) {
                    EventCalculator.logger.log(Level.SEVERE,
                               "Can't parse json payload: "+content, ex);
                }
            }
        }

        // Add back other payload info to event
        if (event != null) {
            Object item = payload.get("InVivoUseRoute");
            String route = parseObject(item);
            //InVivoUseRoute is a controlled vocabularly
            //if the value = "other" than use the field InVivoUseRouteOther
            //which is free text.
            if("other".equalsIgnoreCase(route)) {
                item = payload.get("InVivoUseRouteOther");
                route = parseObject(item);
            }
            if(route !=null && !route.trim().isEmpty()){
                event.route = route;
            }
            item = payload.get("Originator");
            String sponsor = parseObject(item);
            if(sponsor !=null && !sponsor.trim().isEmpty() &&
                !sponsor.equals("Unknown"))
                event.sponsor = sponsor;
            item = payload.get("OriginatorUri");
            String url = parseObject(item);
            if (event.URL != null && url != null &&
                    !url.trim().isEmpty() && !url.trim().equals("Unknown"))
                event.URL = url;
        }
        if (event != null) events.put(String.valueOf(System.identityHashCode(event)), event);
        return;
    }
}
