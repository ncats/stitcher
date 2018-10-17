package ncats.stitcher.calculators.events;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class EventParser {
    final public String name;
    public Map<String,Event> events = new HashMap();

    protected EventParser(String name) {
        this.name = name;
    }

    public abstract void produceEvents(Map<String, Object> payload);
    public void reset() {events = new HashMap();}

    @Deprecated Event fullEvent(String id) {
        Event event = new Event(name, id, Event.EventKind.Marketed);
        event.source = name;
        //event.jurisdiction = "US";
        //event.startDate;
        //event.endDate;
        //event.active;
        //event.URL;
        //event.approvalAppId;
        //event.product;
        //event.sponsor;
        //event.route;
        //event.comment;
        return event;
    }

    Event minimalEvent(String unii,
                       String entry,
                       String type,
                       String product,
                       String url,
                       Event.EventKind kind,
                       String jurisdiction) {
        String id = unii + ":" + type + ":" + entry;
        Event event = new Event(name, id, kind);
        event.source = name;
        if (product != null)
            event.product = product;
        if (url != null)
            event.URL = url;
        event.approvalAppId = type +":"+entry;
        if (jurisdiction != null)
            event.jurisdiction = jurisdiction;
        return event;
    }

    String getFirstEntry(Object obj) {
        if (obj.getClass().isArray()) {
            return (String) Array.get(obj,0);
        }
        return (String)obj;
    }

    class EventFormat {
        String jurisdiction;
        Event.EventKind kind;
        EventFormat(String jurisdiction, Event.EventKind kind) {
            this.jurisdiction = jurisdiction;
            this.kind = kind;
        }
    }

    static HashMap<String, EventFormat> types = new HashMap();
    {
        types.put("HEALTH -CANADA NHP INGREDIENT MONOGRAPH", new EventFormat("Canada", Event.EventKind.Marketed));
        types.put("Canada", new EventFormat("Canada", Event.EventKind.Marketed));
        types.put("UK NHS", new EventFormat("UK", Event.EventKind.Marketed));
        types.put("Japan", new EventFormat("Japan", Event.EventKind.Marketed));
        types.put("ORANGE BOOK", new EventFormat("US", Event.EventKind.Marketed));
        types.put("WHO-ATC", new EventFormat(null, Event.EventKind.Marketed));
        types.put("WHO-ESSENTIAL MEDICINES LIST", new EventFormat(null, Event.EventKind.Marketed));
        types.put("CFR", new EventFormat("US", Event.EventKind.Marketed));
        types.put("FDA ORPHAN DRUG", new EventFormat("US", Event.EventKind.Designation));
        types.put("DEA NO.", new EventFormat("US", Event.EventKind.Other));
        types.put("EU-Orphan Drug", new EventFormat("EU", Event.EventKind.Designation));
        types.put("INN", new EventFormat(null, Event.EventKind.Clinical));
        types.put("USAN", new EventFormat("US", Event.EventKind.Clinical));
        types.put("JAN", new EventFormat("Japan", Event.EventKind.Clinical));
        types.put("BAN", new EventFormat("UK", Event.EventKind.Clinical));
        types.put("GREEN BOOK", new EventFormat("US", Event.EventKind.USAnimalDrug));
        //types.put("EVMPD", new EventFormat("EU", Event.EventKind.Marketed));
        //types.put("MART.", new EventFormat(null, Event.EventKind.Marketed));
        //types.put("WHO-DD", new EventFormat(null, Event.EventKind.Marketed));
        //types.put("MI", new EventFormat(null, Event.EventKind.Marketed));
    }

}
