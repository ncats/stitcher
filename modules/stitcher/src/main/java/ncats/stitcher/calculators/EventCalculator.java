package ncats.stitcher.calculators;

import ncats.stitcher.*;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import ncats.stitcher.calculators.events.*;

import static ncats.stitcher.Props.*;

public class EventCalculator extends StitchCalculator {

    static EventParser[] DEFAULT_EVENT_PARSERS = {
            new GSRSEventParser(),
            new DrugsAtFDAEventParser (),
            new ClinicalTrialsEventParser(),
            new RanchoEventParser(),
            new NPCEventParser (),
            new PharmManuEventParser (),
            //!! No longer loading this source: new DrugBankEventParser (),
            new DrugBankXmlEventParser (),
            new DailyMedRxEventParser (),
            new DailyMedOtcEventParser (),
            new DailyMedRemEventParser (),
            new WithdrawnEventParser()
    };


    public static final Logger logger = Logger.getLogger(Stitch.class.getName());
    public static final SimpleDateFormat SDF = new SimpleDateFormat ("yyyy-MM-dd");
    public static final SimpleDateFormat SDF2 = new SimpleDateFormat ("MM/dd/yyyy");

    final DataSourceFactory dsf;

    private List<EventParser> eventParsers = Arrays.asList(DEFAULT_EVENT_PARSERS);

    // adapted from https://prsinfo.clinicaltrials.gov/definitions.html
    public static Set<String> CLINICAL_PHASES = Arrays.asList(
            "Not Applicable", "Early Phase 1", "Phase 1", "Phase 1/Phase 2",
            "Phase 2", "Phase 2/Phase 3", "Phase 3", "Phase 4")
            .stream()
            .collect(Collectors.toCollection(LinkedHashSet::new));

    public EventCalculator(EntityFactory ef) {
        this.ef = ef;
        this.dsf = ef.getDataSourceFactory();
    }

    public void setEventParsers(List<EventParser> parsers){
        this.eventParsers = new ArrayList<>(Objects.requireNonNull(parsers));
    }

    /*
     * Consumer<Stitch>
     */
    public void accept (Stitch stitch) {
        List<Event> events = getEvents(stitch);
        logger.info("Stitch "+stitch.getId()+" => "+ events.size()+
                    " event events");

        stitch.removeAll(AuxRelType.EVENT.name());
        Labels labels = new Labels();

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
            if (e.source == null) // TODO e.source is coming in as null in some cases
                e.source = "*!";
            props.put(SOURCE, e.source);
            props.put(KIND, e.kind.toString());

            labels.add(e.kind);

            Map<String, Object> data = new HashMap<>();
            for (Field field: e.getClass().getDeclaredFields()) {
                try {
                    if (field.getType() == String.class && field.get(e) != null)
                        data.put(field.getName(), field.get(e));
                    if (field.getType() == Date.class && field.get(e) != null)
                        data.put(field.getName(), SDF.format(field.get(e)));
                } catch (IllegalAccessException ex) {
                    ex.printStackTrace();
                }
            }
            labels.add(e.source);
            if (e.startDate != null && e.kind.isApproved()) {
                cal.setTime(e.startDate);
                approvals.put(e.source, cal.get(Calendar.YEAR));
            }
            
            // now add event to this stitch node
            stitch.addIfAbsent(AuxRelType.EVENT.name(), props, data);
        }

        // Calculate Initial Approval
        Event initUSAppr = null;
        Event initAppr = null;
        for (Event e: events) {
            if (e.startDate != null && "US".equals(e.jurisdiction) && e.kind.isApproved()) {
                if (initAppr == null || e.startDate.before(initAppr.startDate)) {
                    initAppr = e;
                }
                if (initUSAppr == null || e.startDate.before(initUSAppr.startDate)) {
                    initUSAppr = e;
                }
            }
            if (e.startDate != null && (e.kind.isApproved() || e.kind == Event.EventKind.Marketed)) {
                if (initAppr == null || e.startDate.before(initAppr.startDate)) {
                    initAppr = e;
                }
            }
        }

        if (initUSAppr != null) {
            cal.setTime(initUSAppr.startDate);
            stitch.set("approved", Boolean.TRUE);
            stitch.set("approvedYear", cal.get(Calendar.YEAR));
            if (initAppr != initUSAppr)
                System.out.println("Initial US marketing: "+cal.get(Calendar.YEAR));
            cal.setTime(initAppr.startDate);
            // TODO watch out for null jurisdictions
            System.out.println("Initially marketed: " + cal.get(Calendar.YEAR) + " (" + initAppr.jurisdiction + ")");
            if ("1900".equals(cal.get(Calendar.YEAR))) {
                System.out.println("whoops");
            }
        } else if (initAppr != null) {
            cal.setTime(initAppr.startDate);
            System.out.println("Initially marketed: "+cal.get(Calendar.YEAR) + " (" + initAppr.jurisdiction + ")");
            stitch.set("approved", Boolean.FALSE);
        } else {
            stitch.set("approved", Boolean.FALSE);
        }

        stitch.addLabel(labels.getLabelNames());
    }

    static class Labels{
        private boolean isApproved;

        Set<String> labels = new TreeSet<>();

        public void add(Event.EventKind kind){
            if(kind.isApproved()){
                isApproved = true;
            }
            add(kind.name());
        }

        public void add(String label){
            if (label != null)
                labels.add(label);
        }

        public String[] getLabelNames(){
            return labels.toArray(new String[labels.size()]);
        }
    }


    static class DrugBankEventParser extends EventParser {
        public DrugBankEventParser() {
            super ("drugbank-full-annotated.sdf");
        }
            
        public void produceEvents(Map<String, Object> payload) {
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
            if (event != null) events.put(String.valueOf(System.identityHashCode(event)), event);
            return;
        }
    }


    List<Event> getEvents (Stitch stitch) {
        List<Event> events = new ArrayList<>();

        for (EventParser ep : eventParsers) {
            try {
                for(Map<String, Object> payload : stitch.multiplePayloads(ep.name)) {
                    ep.produceEvents(payload);
                }
            } catch (IllegalArgumentException iae) {
                logger.warning(ep.name + " not a valid data source");
            }

        }
        for (EventParser ep : eventParsers) {
            for (Event e: ep.events.values()) {
                logger.info(ep.name + ": kind=" + e.kind
                        + " startDate=" + e.startDate);
                events.add(e);
            }
            ep.reset();
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
        int count;
        if ("1".equals(argv[1]) || "2".equals(argv[1])) {
            int version = Integer.parseInt(argv[1]);
            count = ac.recalculate(version);
        } else {
            long[] nodes = new long[argv.length - 1];
            for (int i = 1; i < argv.length; i++)
                nodes[i - 1] = Long.parseLong(argv[i]);
            count = ac.recalculateNodes(nodes);
        }
        logger.info(count+" stitches recalculated!");
        ef.shutdown();
    }
}
