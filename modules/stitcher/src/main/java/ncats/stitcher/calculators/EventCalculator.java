package ncats.stitcher.calculators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ncats.stitcher.*;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static ncats.stitcher.Props.*;

public class EventCalculator implements StitchCalculator {
    static final Logger logger = Logger.getLogger(Stitch.class.getName());
    static final SimpleDateFormat SDF = new SimpleDateFormat ("yyyy-MM-dd");
    static final SimpleDateFormat SDF2 = new SimpleDateFormat ("MM/dd/yyyy");

    final EntityFactory ef;
    final DataSourceFactory dsf;
    
    public EventCalculator(EntityFactory ef) {
        this.ef = ef;
        this.dsf = ef.getDataSourceFactory();
    }

    public int recalculate (int version) {
        logger.info("## recalculating stitch_v"+version+"...");
        return ef.maps(e -> {
                Stitch s = Stitch._getStitch(e);
                accept (s);
            }, "stitch_v"+version);
    }

    /*
     * Consumer<Stitch>
     */
    public void accept (Stitch stitch) {
        List<Event> events = getEvents(stitch);
        logger.info("Stitch "+stitch.getId()+" => "+ events.size()+
                    " event events");

        stitch.removeAll(AuxRelType.EVENT.name());
        Set<String> labels = new TreeSet<>();
        Calendar cal = Calendar.getInstance();
        
        Map<String, Integer> approvals = new HashMap<>();
        // ensure addIfAbsent will work by making IDs unique
        HashMap<String, Integer> eventIndexes = new HashMap<>(); 
        for (Event e : events) {
            Map<String, Object> props = new HashMap<>();
            String ei = e.source + "|" + e.id;
            if (eventIndexes.containsKey(ei)) {
                props.put(ID, e.id + ":" + eventIndexes.get(ei));
                eventIndexes.put(ei, eventIndexes.get(ei)+1);
            } else {
                props.put(ID, e.id);
                eventIndexes.put(ei, 1);
            }
            props.put(SOURCE, e.source);
            props.put(KIND, e.kind.toString());

            labels.add(e.kind.toString());

            Map<String, Object> data = new HashMap<>();
            if (e.date != null)
                data.put("date", SDF.format(e.date));

            if (e.jurisdiction != null)
                data.put("jurisdiction", e.jurisdiction);

            if (e.comment != null)
                data.put("comment", e.comment);
            
            labels.add(e.source);
            if (e.date != null && e.kind == Event.EventKind.ApprovalRx) {
                cal.setTime(e.date);
                approvals.put(e.source, cal.get(Calendar.YEAR));
            }
            
            // now add event to this stitch node
            stitch.addIfAbsent(AuxRelType.EVENT.name(), props, data);
        }

        stitch.set("approved",
                   labels.contains(Event.EventKind.ApprovalRx.toString()));
        
        // sources that have approval dates; order by relevance
        for (EventParser ep : new EventParser[]{
                new DrugsAtFDAEventParser(),
                new DailyMedEventParser()
            }) {
            Integer year = approvals.get(ep.name);
            if (year != null) {
                stitch.set("approvedYear", year);
                break;
            }
        }
        
        stitch.addLabel(labels.toArray(new String[0]));
    }

    static class Event {
        public EventKind kind;
        public String source;
        public Object id;
        public Date date;
        public String jurisdiction;
        public String comment; // reference

        public enum EventKind {
            Publication,
            Filing,
            Designation,
            ApprovalRx,
            ApprovalOtc,
            Marketed
        }

        public Event(String source, Object id, EventKind kind, Date date) {
            this.source = source;
            this.id = id != null ? id : "*";
            this.kind = kind;
            this.date = date;
        }

        public Event(String source, Object id, EventKind kind) {
            this(source, id, kind, null);
        }
    }

    static abstract class EventParser {
        final public String name;
        protected EventParser(String name) {
            this.name = name;
        }

        public abstract List<Event> getEvents (Map<String, Object> payload);
    }

    static class RanchoEventParser extends EventParser {
        final ObjectMapper mapper = new ObjectMapper ();
        final Base64.Decoder decoder = Base64.getDecoder();
        Event event;
        Object id;

        public RanchoEventParser() {
            super ("rancho_2017-09-07_20-32.json");
        }

        void parseEvent (JsonNode n) {
            if (n.has("HighestPhase") && "approved".equalsIgnoreCase
                (n.get("HighestPhase").asText())) {
                event = new Event(name, id, Event.EventKind.Marketed);
                if (n.has("HighestPhaseUri")) {
                    event.comment = n.get("HighestPhaseUri").asText();
                    if (event.comment.contains("fda.gov")) {
                        event.jurisdiction = "US";
                    }
                }
                else {
                    event.comment = "";
                    if (n.has("ConditionName"))
                        event.comment +=
                                n.get("ConditionName").asText();
                    else if (n.has("ConditionMeshValue"))
                        event.comment +=
                                n.get("ConditionMeshValue").asText();
                    if (n.has("ConditionProductName"))
                        event.comment += " " + n.get("ConditionProductName").asText();
                }
                if (n.has("ConditionProductDate")) {
                    String d = n.get("ConditionProductDate").asText();
                    try {
                        Date date = SDF.parse(d);
                            event.date = date;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        logger.log(Level.SEVERE,
                                "Can't parse date: "+d, ex);
                    }
                }
            }
        }

        void parseEvent (String content) throws Exception {
            JsonNode node = mapper.readTree(decoder.decode(content));
            if (node.isArray()) {
                for (int i = 0; i < node.size(); ++i) {
                    JsonNode n = node.get(i);
                    parseEvent (n);
                }
            }
            else 
                parseEvent (node);
        }

        public List<Event> getEvents(Map<String, Object> payload) {
            List<Event> events = new ArrayList<>();
            event = null;
            id = payload.get("Unii");
            if (id != null && id.getClass().isArray()
                && Array.getLength(id) == 1)
                id = Array.get(id, 0);

            Object content = payload.get("Conditions");
            if (content != null) {
                if (content.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(content); ++i) {
                        String c = (String) Array.get(content, i);
                        try {
                            parseEvent (c);
                        }
                        catch (Exception ex) {
                            logger.log(Level.SEVERE, 
                                       "Can't parse Json payload: "+c, ex);
                        }
                    }
                }
                else {
                    try {
                        parseEvent ((String)content);
                    }
                    catch (Exception ex) {
                        logger.log(Level.SEVERE, 
                                   "Can't parse json payload: "+content, ex);
                    }
                }
            }
            if (event != null) events.add(event);
            return events;
        }
    }
        
    static class NPCEventParser extends EventParser {
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
    
    static class PharmManuEventParser extends EventParser {
        final ObjectMapper mapper = new ObjectMapper ();
        final Base64.Decoder decoder = Base64.getDecoder();
            
        public PharmManuEventParser() {
            super ("PharmManuEncycl3rdEd.json");
        }
            
        public List<Event> getEvents(Map<String, Object> payload) {
            List<Event> events = new ArrayList<>();
            Object id = payload.get("UNII");

            String content = (String) payload.get("Drug Products");
            if (content != null) {
                try {
                    for (String c : content.split("\n")) {
                        JsonNode node = mapper.readTree(decoder.decode(c));
                        Event event = new Event (name, id, Event.EventKind.Marketed);
                        if (node.has("Year Introduced")) {
                            String year = node.get("Year Introduced").asText();
                            try {
                                Date date = SDF.parse(year+"-12-31");
                                event.date = date;
                            }
                            catch (Exception ex) {
                                logger.log(Level.SEVERE, 
                                           "Can't parse date: "+year, ex);
                            }
                        }
                        if (node.has("Country")) {
                            event.jurisdiction =
                                    node.get("Country").asText();
                            if (event.jurisdiction.equals("US"))
                                event.kind = Event.EventKind.ApprovalRx;
                        }
                        if (node.has("Product")) {
                            event.comment =
                                    node.get("Product").asText();
                        }
                        if (node.has("Company")) {
                            if (event.comment.length() > 0)
                                event.comment += " [" + node.get("Company").asText() + "]";
                            else event.comment +=
                                    node.get("Company").asText();
                        }
                        events.add(event);
                    }
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, 
                               "Can't parse json: '"+content+"'", ex);
                }
            }

            return events;
        }
    }
        
    static class DrugBankEventParser extends EventParser {
        public DrugBankEventParser() {
            super ("drugbank-full-annotated.sdf");
        }
            
        public List<Event> getEvents(Map<String, Object> payload) {
            List<Event> events = new ArrayList<>();
            Event event = null;
            Object id = payload.get("DATABASE_ID");
            if (id != null && id.getClass().isArray()
                && Array.getLength(id) == 1)
                id = Array.get(id, 0);
            
            Object content = payload.get("DRUG_GROUPS");
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
                else if (((String)content)
                         .toLowerCase().indexOf("approved") >= 0) {
                    event = new Event(name, id, Event.EventKind.Marketed);
                    event.comment = (String)content;
                }
            }
            if (event != null) events.add(event);
            return events;
        }
    }

    static class DailyMedEventParser extends EventParser {
        public DailyMedEventParser() {
            super ("spl_acti_rx.txt");
        }

        public List<Event> getEvents(Map<String, Object> payload) {
            List<Event> events = new ArrayList<>();
            Event event = null;
            Object id = payload.get("NDC");

            Object content = null;
            Date date = null;
            try {
                content = payload.get("InitialYearApproval");
                if (content != null) {
                    date = SDF.parse((String) content + "-12-31");
                }
                content = payload.get("MarketDate");
                if (content != null) {
                    Date date2 = SDF.parse((String)content);
                    if (date == null || date.after(date2))
                        date = date2;
                }
                if (date != null) {
                    Event.EventKind et = Event.EventKind.Marketed;
                    content = payload.get("ApprovalAppId");
                    if (content != null &&
                            (((String)content).startsWith("NDA") ||
                            ((String)content).startsWith("ANDA") ||
                            ((String)content).startsWith("BA") ||
                            ((String)content).startsWith("BN") ||
                            ((String)content).startsWith("BLA")))
                        et = Event.EventKind.ApprovalRx;
                    else {
                        logger.log(Level.WARNING, "Unusual approval app id: "+content
                                   +": "+payload.toString());
                    }
                    event = new Event(name, id, et);
                    event.jurisdiction = "US";
                    event.date = date;
                    event.comment = (String) payload.get("ApprovalAppId");
                }
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't parse date: "+content, ex);
            }

            if (event != null) events.add(event);
            return events;
        }
    }

    static class DrugsAtFDAEventParser extends EventParser {
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
                    Date date = SDF2.parse((String)content);
                    if (event == null
                        || (event.kind == Event.EventKind.ApprovalRx
                        && event.date.after(date))) {
                        event = new Event(name, id, Event.EventKind.ApprovalRx);
                        event.date = date;
                        event.jurisdiction = "US";
                        event.comment = (String) payload.get("Comment");
                    }
                }
                catch (Exception ex) {
                    logger.log
                        (Level.SEVERE, "Unknown date format: "+content, ex);
                }
            }

            if (event != null) events.add(event);
            return events;
        }       
    }

    static EventParser[] eventParsers = {
        new RanchoEventParser(),
        new NPCEventParser(),
        new PharmManuEventParser(),
        new DrugBankEventParser(),
        new DailyMedEventParser(),
        new DrugsAtFDAEventParser()
    };

    List<Event> getEvents (Stitch stitch) {
        List<Event> events = new ArrayList<>();

        for (EventParser ep : eventParsers) {
            try {
                Map<String, Object> payload = stitch.payload(ep.name);
                if (payload != null) {
                    for (Event e: ep.getEvents(payload)) {
                        logger.info(ep.name + ": kind=" + e.kind
                                + " date=" + e.date);
                        events.add(e);
                    }
                }
            } catch (IllegalArgumentException iae) {
                logger.warning(ep.name + " not a valid data source");
            }
        }

        return events;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+EventCalculator.class.getName()
                               +" DB VERSION");
            System.exit(1);
        }

        EntityFactory ef = new EntityFactory (GraphDb.getInstance(argv[0]));
        EventCalculator ac = new EventCalculator(ef);
        int version = Integer.parseInt(argv[1]);
        int count = ac.recalculate(version);
        logger.info(count+" stitches recalculated!");
        ef.shutdown();
    }
}
