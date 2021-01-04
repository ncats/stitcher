package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.lang.reflect.Array;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

public class DrugsAtFDAEventParser extends EventParser {
    public DrugsAtFDAEventParser() {
        super ("Drugs@FDA & OB, December 2020");
    }

    public void produceEvents(Map<String, Object> payload) {
        Event event = null;
        Object id = payload.get("UNII");
        Object content = payload.get("Date");
        if (content != null) {
            try {
                Date date = EventCalculator.SDF2.parse((String)content);
                event = new Event(name, id, Event.EventKind.USPreviouslyMarketed);
                event.jurisdiction = "US";
                event.startDate = date;
                //event.endDate;
                event.active = (String) payload.get("active");
                Object source = payload.get("Date_Method");
                if (source != null && source.getClass().isArray()) {
                    event.source = (String) Array.get(source,0);
                } else {
                    event.source = (String) source;
                }
                Object url = payload.get("Url");
                if (url != null && url.getClass().isArray()) {
                    event.URL = (String) Array.get(url,0);
                } else {
                    event.URL = (String) url;
                }
                Object appType = payload.get("App_Type");
                if ("true".equals(event.active)) {
                    event.approvalAppId = (String) payload.get("App_Type") +
                            (String) payload.get("App_No");
                    event.kind = Event.EventKind.USApprovalRx;
                }
                event.product = (String) payload.get("Product");
                event.sponsor = (String) payload.get("Sponsor");
                //event.route;
                event.comment = (String) payload.get("Comment");
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE,
                           "Can't parse "+id+" startDate: \""+content+"\"", ex);
            }
        }

        if (event != null) events.put(String.valueOf(System.identityHashCode(event)), event);
        return;
    }
}
