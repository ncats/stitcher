package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class FDAanimalDrugsParser extends EventParser {
    public FDAanimalDrugsParser() {
        super ("FDA NADA and ANADAs, December 2018");
    }

    public void produceEvents(Map<String, Object> payload) {
        Event event = null;
        Object id = payload.get("UNII");
        if (id != null) {
            try {
                event = new Event(name, id, Event.EventKind.Other);
                event.jurisdiction = "US";
                //event.startDate;
                //event.endDate; TODO Put previously marketed end dates into OTC file
                //event.active = (String) payload.get("active");
                String status = (String) payload.get("Status");
                if ("Approved".equals(status)) {
                    event.kind = Event.EventKind.USAnimalDrug;
                    event.active = "true";
                } else if ("Voluntarily Withdrawn".equals(status)) {
                    event.kind = Event.EventKind.Other;
                    event.active = "false";
                } else if ("Withdrawn".equals(status)) {
                    event.kind = Event.EventKind.Other;
                    event.active = "false";
                }
                event.source = this.name;
                event.URL = (String) payload.get("Reference");
                event.product = (String) payload.get("Trade Name");
                event.sponsor = (String) payload.get("Sponsor");
                String app = "A".equals(payload.get("AppType")) ? "ANADA" : "NADA";
                if ("NADA".equals(app) && ((String)payload.get("AppNo")).startsWith("200"))
                    app = "ANADA";
                event.approvalAppId = app + " " + (String) payload.get("AppNo") + " "
                        + event.sponsor + " " + event.product;
                Object routes = payload.get("Routes");
                if (routes != null) {
                    if (routes.getClass().isArray())
                        event.route = Arrays.toString((Object[]) routes);
                    else event.route = routes.toString();
                }
                event.comment = event.approvalAppId + " " + payload.get("Rx") + " " + payload.get("Ingredient");
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE,
                           "Can't parse id: \""+id+"\"", ex);
            }
        }

        if (event != null) events.put(String.valueOf(System.identityHashCode(event)), event);
        return;
    }
}
