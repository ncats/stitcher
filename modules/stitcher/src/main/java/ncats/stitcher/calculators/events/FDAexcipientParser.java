package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.Map;
import java.util.logging.Level;

public class FDAexcipientParser extends EventParser {
    public FDAexcipientParser() {
        super ("FDAexcipients.txt");
    }

    public void produceEvents(Map<String, Object> payload) {
        Event event = null;
        Object id = payload.get("UNII");
        if (id != null) {
            try {
                event = new Event(name, id, Event.EventKind.Excipient);
                event.jurisdiction = "US";
                //event.startDate;
                //event.endDate; TODO Put previously marketed end dates into OTC file
                event.active = "true";
                event.source = this.name;
                event.URL = (String) payload.get("IIG-URL");
                event.product = (String) payload.get("Strength");
                //event.sponsor = (String) payload.get("Sponsor");
                event.approvalAppId = event.product;
                event.route = (String) payload.get("Route");
                event.comment = event.approvalAppId;
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
