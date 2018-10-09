package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class ClinicalTrialsEventParser extends EventParser {
    boolean visited = false;

    public ClinicalTrialsEventParser() {
        super ("NCT_REPORT.txt.gz");
    }

    public List<Event> getEvents(Map<String, Object> payload) {
        List<Event> events = new ArrayList<>();
        if (visited) return events;
        visited = true;
        Event event = null;
        Object id = payload.get("UNII");
        Object status = payload.get("STATUS");
        if ("Phase 4".equals(status)) {
            try {
                Object dateobj = payload.get("DATE");
                Date date = EventCalculator.SDF.parse((String)dateobj);
                event = new Event(name, id, Event.EventKind.Marketed);
                //event.jurisdiction = "US";
                event.startDate = date;
                //event.endDate;
                //event.active;
                event.source = name;
                event.URL = "https://clinicaltrials.gov/ct2/show/"+payload.get("NCT_ID");
                //event.approvalAppId;
                event.product = payload.get("NCT_ID") + ": " + status + " " + (String) payload.get("CONDITION");
                //event.sponsor;
                //event.route;
                //event.withDrawnYear;
                //event.marketingStatus;
                //event.productCategory;
                //event.comment;
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE,
                           "Can't parse clinicalTrials.gov entry: \""+id+"\"", ex);
            }
        }

        if (event != null) events.add(event);
        return events;
    }
}
