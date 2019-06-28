package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

public class OTCMonographParser extends EventParser {
    public OTCMonographParser() {
        super ("OTC Monographs, December 2018");
    }

    public void produceEvents(Map<String, Object> payload) {
        Event event = null;
        Object id = payload.get("UNII");
        if (id != null) {
            try {
                event = new Event(name, id, Event.EventKind.USPreviouslyMarketed);
                event.jurisdiction = "US";
                //event.startDate;
                //event.endDate; TODO Put previously marketed end dates into OTC file
                //event.active = (String) payload.get("active");
                String status = (String) payload.get("OTC_Status");
                if ("Approved".equals(status))
                    event.kind = Event.EventKind.USApprovalOTC;
                else if ("Marketed".equals(status))
                    event.kind = Event.EventKind.Marketed;
                else if ("Animal Drug".equals(status))
                    event.kind = Event.EventKind.USAnimalDrug;
                else if ("Animal Dietary Supplement".equals(status))
                    event.kind = Event.EventKind.Other;
                event.source = this.name;
                event.URL = (String) payload.get("OTC_URL");
                event.product = (String) payload.get("OTC_Ingredient");
                event.sponsor = (String) payload.get("OTC_Monograph");
                event.approvalAppId = (String) payload.get("OTC_Ref_Text") + " "
                        + event.sponsor + " " + event.product;
                //event.route;
                //event.comment;
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
