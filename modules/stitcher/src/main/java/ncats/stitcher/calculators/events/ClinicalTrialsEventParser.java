package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.*;
import java.util.logging.Level;

public class ClinicalTrialsEventParser extends EventParser {

    public ClinicalTrialsEventParser() {
        super ("ClinicalTrials, February 2021");
    }

    boolean earlyEvent(Event event, String ctphase) {
        if (EventCalculator.CLINICAL_PHASES.contains(ctphase)) {
            for (String phase : EventCalculator.CLINICAL_PHASES) {
                if (phase.equals(ctphase)) {
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
            String phase = (String) payload.get("PHASE");
            Event.EventKind ek = Event.EventKind.Clinical;
            if ("Phase 4".equals(phase))
                ek = Event.EventKind.Marketed;
            event = new Event(name, id, ek);
            //event.jurisdiction;
            Object dateobj = payload.get("START");
            Object dateobj2 = payload.get("END");
            try {
                if (dateobj != null) {
                    Date date = EventCalculator.SDF.parse((String) dateobj);
                    event.startDate = date;
                }
                if (dateobj2 != null) {
                    Date date2 = EventCalculator.SDF.parse((String) dateobj2);
                    event.endDate = date2;
                }
            } catch (Exception exc) {
                EventCalculator.logger.log(Level.SEVERE,
                        "Can't parse clinicalTrials.gov entry date: \"" + (String)dateobj + "\"", exc);
            }
            //event.active;
            event.source = name;
            String ctid = payload.get("NCT_ID").toString();
            event.URL = payload.get("URL").toString();
            if (event.URL.startsWith("https://clinicaltrials.gov/ct2/show/") && !ctid.startsWith("NCT0"))
                event.URL = "http://apps.who.int/trialsearch/Trial3.aspx?trialid=" + ctid;
            //event.approvalAppId;
            String ttype = payload.get("TYPE").toString();
            String status = payload.get("STATUS").toString();
            String condition = payload.get("CONDITION").toString();
            String title = payload.get("TITLE").toString();
            event.product = payload.get("NCT_ID") + ": " + phase + " " + ttype + " " + status + " " + condition;
            //event.sponsor;
            //event.route;
            event.comment = title;

            if (earlyEvent(event, phase))
                events.put(status, event);
        } catch (Exception ex) {
            EventCalculator.logger.log(Level.SEVERE,
                    "Can't parse clinicalTrials.gov entry: \"" + id + "\"", ex);
        }

        return;
    }
}
