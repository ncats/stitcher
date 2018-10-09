package ncats.stitcher.calculators.events;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NPCEventParser extends EventParser {
    public NPCEventParser() {
        super ("npc-dump-1.2-04-25-2012_annot.sdf.gz");
    }

    public void produceEvents(Map<String, Object> payload) {
        String id = getFirstEntry(payload.get("ID"));
        for (String ref: (String[])payload.get("REFS")) {
            String[] sref = ref.split("\\|");
            String type = sref[2];
            String product = null;
            if ("Canada".equals(type))
                product = sref[1];
            String ingred = sref[0];
            if (types.containsKey(type)) {
                types.get(type);
                Event event = minimalEvent("NPC:"+id+":"+ingred, ingred, type, product,null,
                        types.get(type).kind, types.get(type).jurisdiction);
                events.put(type, event);
            }
        }

//        if (content != null) {
//            if (content.getClass().isArray()) {
//                StringBuilder approved = new StringBuilder ();
//                for (int i = 0; i < Array.getLength(content); ++i) {
//                    String s = (String) Array.get(content, i);
//                    if (s.toLowerCase().indexOf("approved") >= 0) {
//                        if (approved.length() > 0)
//                            approved.append("; ");
//                        approved.append(s);
//                    }
//                }
//
//                if (approved.length() > 0) {
//                    event = new Event(name, id, Event.EventKind.Marketed);
//                    event.comment = approved.toString();
//                }
//            }
//            else if (((String)content).toLowerCase()
//                       .indexOf("approved") >= 0) {
//                event = new Event(name, id, Event.EventKind.Marketed);
//                event.comment = (String)content;
//            }
//        }
//
//        if (event != null) events.put(String.valueOf(System.identityHashCode(event)), event);
        return;
    }
}
