package ncats.stitcher.calculators.events;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NPCEventParser extends EventParser {
    public NPCEventParser() {
        super ("npc-dump-1.2-04-25-2012_annot.sdf.gz");
    }

    public List<Event> getEvents(Map<String, Object> payload) {
        List<Event> events = new ArrayList<>();
        Event event = null;
        Object content = payload.get("DATASET");
        Object id = payload.get("ID");
        if (id != null && id.getClass().isArray()
            && Array.getLength(id) == 1)
            id = Array.get(id, 0);

        if (content != null) {
            if (content.getClass().isArray()) {
                StringBuilder approved = new StringBuilder ();
                for (int i = 0; i < Array.getLength(content); ++i) {
                    String s = (String) Array.get(content, i);
                    if (s.toLowerCase().indexOf("approved") >= 0) {
                        if (approved.length() > 0)
                            approved.append("; ");
                        approved.append(s);
                    }
                }

                if (approved.length() > 0) {
                    event = new Event(name, id, Event.EventKind.Marketed);
                    event.comment = approved.toString();
                }
            }
            else if (((String)content).toLowerCase()
                       .indexOf("approved") >= 0) {
                event = new Event(name, id, Event.EventKind.Marketed);
                event.comment = (String)content;
            }
        }

        if (event != null) events.add(event);
        return events;
    }
}
