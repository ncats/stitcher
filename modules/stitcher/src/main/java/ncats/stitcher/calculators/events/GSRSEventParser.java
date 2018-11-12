package ncats.stitcher.calculators.events;

import java.lang.reflect.Array;
import java.util.Map;

public class GSRSEventParser extends EventParser {
    public GSRSEventParser() {
        super ("dump-public-2018-07-19.gsrs");
    }

    public void produceEvents(Map<String, Object> payload) {
        String unii = getFirstEntry(payload.get("UNII"));
        Object syn = payload.get("Synonyms");
        for (String type: types.keySet()) {
            if (payload.containsKey(type)) {
                String entry = getFirstEntry(payload.get(type));
                Event event = minimalEvent(unii, entry, type, null,
                        "https://ginas.ncats.nih.gov/ginas/app/substance/"+unii,
                        types.get(type).kind, types.get(type).jurisdiction);
                events.put(type, event);
            }
        }
        if (syn != null && syn.getClass().isArray())
        for (int i=0; i<Array.getLength(syn); i++) {
            String name = Array.get(syn, i).toString();
            if (name.indexOf(" [") > -1) {
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
