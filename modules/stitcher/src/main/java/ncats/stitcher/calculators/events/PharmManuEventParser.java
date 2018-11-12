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

    public void produceEvents(Map<String, Object> payload) {
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
                            event.startDate = date;
                        }
                        catch (Exception ex) {
                            EventCalculator.logger.log(Level.SEVERE,
                                       "Can't parse startDate: "+year, ex);
                        }
                    }
                    if (node.has("Country")) {
                        event.jurisdiction =
                                node.get("Country").asText();
                        /* removed on 2018-08-30 as there are some bogus entries
                        if (event.jurisdiction.equals("US"))
                            event.kind = Event.EventKind.ApprovalRx;
                        */
                    }
                    if (node.has("Product")) {
                        event.comment =
                                node.get("Product").asText();
                        event.product = node.get("Product").asText();
                    }
                    if (node.has("Company")) {
                        event.sponsor = node.get("Company").asText();
                        if (event.comment.length() > 0)
                            event.comment += " [" + node.get("Company").asText() + "]";
                        else event.comment +=
                                node.get("Company").asText();
                    }
                    event.URL = "https://archive.org/details/Pharmaceutical_Manufacturing_Encyclopedia_Vols_12_2nd_Ed";
                    events.put(String.valueOf(System.identityHashCode(event)), event);
                }
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE,
                           "Can't parse json: '"+content+"'", ex);
            }
        }

        return;
    }
}
