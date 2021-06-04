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
        super ("FRDB, May 2021");
    }

    void parseCondition(JsonNode n) {
        if (n.has("condition_highest_phase") 
            && n.get("condition_highest_phase") != null 
            && ("approved".equalsIgnoreCase(n.get("condition_highest_phase").asText()) 
                || ("phase IV".equalsIgnoreCase(n.get("condition_highest_phase").asText()))
                )
            ) {
            
            event = new Event(name, id, Event.EventKind.Marketed);
            
            if (n.has("highest_phase_uri")) {
                event.URL = n.get("highest_phase_uri").asText();
            
                if (event.URL.contains("fda.gov")) {
                    event.jurisdiction = "US";
                    // While Rancho is manually curated, they shouldn't override annotations directly from FDA
                    // Such annotations need to be manually reviewed
//                    if (event.URL.contains("Veterinary") || event.URL.contains("Animal"))
//                        event.kind = Event.EventKind.USAnimalDrug;
//                    else
//                        event.kind = Event.EventKind.USApprovalRx;
                } else if (event.URL.contains("dailymed.nlm.nih.gov")) {
                    event.jurisdiction = "US";
                    // While Rancho is manually curated, they shouldn't override annotations directly from FDA
                    // Such annotations need to be manually reviewed
//                    event.kind = Event.EventKind.USUnapproved; // TODO Update Rancho highest phase to match new nomenclature
                }
            } else {
                event.comment = "";
              
                if (n.has("name")) {
                    event.comment +=
                            n.get("name").asText();
                } else if (n.has("condition_mesh")) {
                    event.comment +=
                            n.get("condition_mesh").asText();
                }
                
                if (n.has("product_name")) {
                    event.comment += " "
                                     + n.get("product_name").asText();
                }
            }

            if (n.has("product_name")) {
                event.product = n.get("product_name").asText();
            }

            if (n.has("product_date")) {
                String d = n.get("product_date").asText();
            
                if (!"unknown".equalsIgnoreCase(d)) {
                    try {
                        Date date = EventCalculator.SDF.parse(d);
                            //event.startDate = date;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        EventCalculator.logger.log(Level.SEVERE,
                                                   "Can't parse startDate: " + d, 
                                                   ex);
                    }
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
        } else {
            parseCondition(node);
        }
    }

    String parseObject (Object item) {
        if (item != null) {
            String value = "";

            if (item.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(item); i++) {
                    value += (value.length() > 0 ? ";" : "") 
                             + Array.get(item, i).toString();
                }
            } else {
                value = item.toString();
            }
            return value;
        }
        return null;
    }

    public void produceEvents(Map<String, Object> payload) {
        event = null;
        id = payload.get("unii");

        if (id != null && id.getClass().isArray()) {
            //&& Array.getLength(id) == 1)
            id = Array.get(id, 0); //TODO Make Rancho UNII unique, for now assume first UNII is good surrogate
        }

        // First, find highest phase product from conditions list
        Object content = payload.get("conditions");
        if (content != null) {
            if (content.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(content); ++i) {
                    String c = (String) Array.get(content, i);

                    try {
                        parseCondition(c);
                    } catch (Exception ex) {
                        EventCalculator.logger.log(Level.SEVERE,
                                                   "Can't parse Json payload: " + c, 
                                                   ex);
                    }
                }
            } else {
                try {
                    parseCondition((String)content);
                } catch (Exception ex) {
                    EventCalculator.logger.log(Level.SEVERE,
                               "Can't parse json payload: "+content, ex);
                }
            }
        }

        // Add back other payload info to event
        if (event != null) {
            Object item = payload.get("in_vivo_use_route");
            String route = parseObject(item);
            //in_vivo_use_route is a controlled vocabularly
            //if the value = "other" than use the field in_vivo_use_route_other
            //which is free text.

            if ("other".equalsIgnoreCase(route)) {
                item = payload.get("in_vivo_use_route_other");
                route = parseObject(item);
            }

            if (route !=null 
                && !route.trim().isEmpty()) {
                
                event.route = route;
            }

            item = payload.get("originator");
            String sponsor = parseObject(item);

            if (sponsor != null && !sponsor.trim().isEmpty() 
                && !sponsor.equalsIgnoreCase("unknown")) {
                
                event.sponsor = sponsor;
            }

            item = payload.get("originator_uri");
            String url = parseObject(item);
            
            if (event.URL != null 
                && url != null 
                && !url.trim().isEmpty() && !url.trim().equalsIgnoreCase("unknown")) {
                
                event.URL = url;
            }
        }
        
        if (event != null) {
            events.put(String.valueOf(System.identityHashCode(event)), event);
        }

        return;
    }
}
