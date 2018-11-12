package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class DrugsAtFDAEventParser extends EventParser {
    public DrugsAtFDAEventParser() {
        super ("approvalYears.txt");
    }

    public void produceEvents(Map<String, Object> payload) {
        Event event = null;
        Object id = payload.get("UNII");
        Object content = payload.get("Date");
        if (content != null) {
            try {
                Date date = EventCalculator.SDF2.parse((String)content);
                event = new Event(name, id, Event.EventKind.Marketed);
                event.jurisdiction = "US";
                event.startDate = date;
                //event.endDate;
                event.active = (String) payload.get("active");
                event.source = (String) payload.get("Date_Method");
                event.URL = (String) payload.get("Url");
                Object appType = payload.get("App_Type");
                if (appType != null) {
                    event.approvalAppId = (String) payload.get("App_Type") +
                            (String) payload.get("App_No");
                    event.kind = Event.EventKind.ApprovalRx;
                }
                event.product = (String) payload.get("Product");
                event.sponsor = (String) payload.get("Sponsor");
                //event.route;
                event.comment = (String) payload.get("Comment");
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE,
                           "Can't parse startDate: \""+content+"\"", ex);
            }
        }

        if (event != null) events.put(String.valueOf(System.identityHashCode(event)), event);
        return;
    }
}
