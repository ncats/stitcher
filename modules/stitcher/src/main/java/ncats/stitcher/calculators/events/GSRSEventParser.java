package ncats.stitcher.calculators.events;

import java.lang.reflect.Array;
import java.util.Map;

public class GSRSEventParser extends EventParser {
    public GSRSEventParser() {
        super ("G-SRS, December 2022");
    }

    public void produceEvents(Map<String, Object> payload) {
        if (payload.get("UNII") == null) {
            // alternative definitions and subconcepts do not get UNIIs
            return;
        }
        String unii = getFirstEntry(payload.get("UNII"));
        for (String type: types.keySet()) {
            if (payload.containsKey(type)) {
                String entry = getFirstEntry(payload.get(type));
                Event event = minimalEvent(unii, entry, type, null,
                        "https://ginas.ncats.nih.gov/ginas/app/substance/"+unii,
                        types.get(type).kind, types.get(type).jurisdiction);
                if ("CFR".equals(type)) { // special cases
                    if (entry == null)
                        event.kind = Event.EventKind.Other;
                    if (entry.startsWith("21 CFR 5")) // animal drug
                        event.kind = Event.EventKind.USAnimalDrug;
                    else if (entry.startsWith("21 CFR 2") || entry.startsWith("21 CFR 3") || entry.startsWith("21 CFR 6"))
                        event.kind = Event.EventKind.Marketed; // TODO this should be done better
                    else event.kind = Event.EventKind.Other;
                }
                events.put(type, event);
            }
        }
        Object syn = payload.get("Synonyms");
        if (syn != null && syn.getClass().isArray())
        for (int i=0; i<Array.getLength(syn); i++) {
            String name = Array.get(syn, i).toString();
            if (name.indexOf(" [") > -1 && name.lastIndexOf(']') > name.lastIndexOf(" [")) {
                String suffix = name.substring(
                        name.lastIndexOf(" [")+2,
                        name.lastIndexOf(']'));
                if (types.containsKey(suffix)) {
                    Event event = minimalEvent(unii, name, suffix, null,
                            "https://ginas.ncats.nih.gov/ginas/app/substance/"+unii,
                            types.get(suffix).kind, types.get(suffix).jurisdiction);
                    events.put(suffix, event);
                }
            }
        }

        return;
    }
}
