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

    public List<Event> getEvents(Map<String, Object> payload) {
        List<Event> events = new ArrayList<>();
        Event event = null;
        Object id = payload.get("UNII");
        Object content = payload.get("Date");
        if (content != null) {
            try {
                Date date = EventCalculator.SDF2.parse((String)content);
                if (event == null
                    || (event.kind.isApproved()
                        && event.date.after(date))) {
                    event = new Event(name, id, Event.EventKind.ApprovalRx);
                    event.date = date;
                    event.jurisdiction = "US";
                    event.comment = (String) payload.get("Comment");
                }
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE,
                           "Can't parse date: \""+content+"\"", ex);
            }
        }

        if (event != null) events.add(event);
        return events;
    }
}
