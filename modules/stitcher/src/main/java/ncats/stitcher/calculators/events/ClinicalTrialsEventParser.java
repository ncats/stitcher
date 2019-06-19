package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.*;
import java.util.logging.Level;

public class ClinicalTrialsEventParser extends EventParser {

    public ClinicalTrialsEventParser() {
        super ("NCT_REPORT.txt.gz");
    }

    boolean earlyEvent(Event event, String status) {
        if (EventCalculator.CLINICAL_PHASES.contains(status)) {
            for (String phase : EventCalculator.CLINICAL_PHASES) {
                if (phase.equals(status)) {
                    if (events.containsKey(phase)) {
                        if (events.get(phase).startDate.after(event.startDate)) {
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return true;
                    }
                }
                else if (events.containsKey(phase) &&
                        events.get(phase).startDate.after(event.startDate)) {
                    events.remove(phase);
                }
            }
        }
        return false;
    }

    public void produceEvents(Map<String, Object> payload) {
        Event event = null;
        Object id = payload.get("UNII");
        try {
            String status = (String) payload.get("STATUS");
            Object dateobj = payload.get("DATE");
            Date date = EventCalculator.SDF.parse((String) dateobj);
            Event.EventKind ek = Event.EventKind.Clinical;
            if ("Phase 4".equals(status))
                ek = Event.EventKind.Marketed;
            event = new Event(name, id, ek);
            //event.jurisdiction;
            event.startDate = date;
            //event.endDate;
            //event.active;
            event.source = name;
            event.URL = "https://clinicaltrials.gov/ct2/show/" + payload.get("NCT_ID");
            //event.approvalAppId;
            event.product = payload.get("NCT_ID") + ": " + status + " " + (String) payload.get("CONDITION");
            //event.sponsor;
            //event.route;
            event.comment = status;

            if (earlyEvent(event, status))
                events.put(status, event);
        } catch (Exception ex) {
            EventCalculator.logger.log(Level.SEVERE,
                    "Can't parse clinicalTrials.gov entry: \"" + id + "\"", ex);
        }

        return;
    }
}
