package ncats.stitcher.calculators.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ncats.stitcher.calculators.EventCalculator;

import java.util.*;
import java.util.logging.Level;

public class PharmManuEventParser extends EventParser {
    final ObjectMapper mapper = new ObjectMapper ();
    final Base64.Decoder decoder = Base64.getDecoder();

    public PharmManuEventParser() {
        super ("PharmManuEncycl3rdEd.json");
    }

    public List<Event> getEvents(Map<String, Object> payload) {
        List<Event> events = new ArrayList<>();
        Object id = payload.get("UNII");

        String content = (String) payload.get("Drug Products");
        if (content != null) {
            try {
                for (String c : content.split("\n")) {
                    JsonNode node = mapper.readTree(decoder.decode(c));
                    Event event = new Event (name, id, Event.EventKind.Marketed);
                    if (node.has("Year Introduced")) {
                        String year = node.get("Year Introduced").asText();
                        try {
                            Date date = EventCalculator.SDF.parse(year+"-12-31");
                            event.date = date;
                        }
                        catch (Exception ex) {
                            EventCalculator.logger.log(Level.SEVERE,
                                       "Can't parse date: "+year, ex);
                        }
                    }
                    if (node.has("Country")) {
                        event.jurisdiction =
                                node.get("Country").asText();
                        if (event.jurisdiction.equals("US"))
                            event.kind = Event.EventKind.ApprovalRx;
                    }
                    if (node.has("Product")) {
                        event.comment =
                                node.get("Product").asText();
                    }
                    if (node.has("Company")) {
                        if (event.comment.length() > 0)
                            event.comment += " [" + node.get("Company").asText() + "]";
                        else event.comment +=
                                node.get("Company").asText();
                    }
                    events.add(event);
                }
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE,
                           "Can't parse json: '"+content+"'", ex);
            }
        }

        return events;
    }
}
