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
            new OTCMonographParser (),
            new FDAanimalDrugsParser (),
            new FDAexcipientParser(),
            new ClinicalTrialsEventParser(),
            new RanchoEventParser(),
            new NPCEventParser (),
            new PharmManuEventParser (),
            new DrugBankXmlEventParser (),
            new DailyMedRxEventParser (),
            new DailyMedOtcEventParser (),
            new DailyMedRemEventParser (),
            new WithdrawnEventParser()
    };


    public static final Logger logger = Logger.getLogger(Stitch.class.getName());
    public static final SimpleDateFormat SDF = new SimpleDateFormat ("yyyy-MM-dd");
    public static final SimpleDateFormat SDF2 = new SimpleDateFormat ("MM/dd/yyyy");

    public static String highestPhase = "highestPhase";
    public static String USapproved = "USapproved"; // boolean of whether active ingredient of currently approved product in US
    public static String initiallyMarketedUS = "initiallyMarketedUS"; // year of initial marketing in the US
    public static String initiallyMarketed = "initiallyMarketed"; // year of initial marketing, anywhere
    public static String initiallyMarketedJurisdiction = "initiallyMarketedJurisdiction"; // where it was initially marketed

    final DataSourceFactory dsf;

    private List<EventParser> eventParsers = Arrays.asList(DEFAULT_EVENT_PARSERS);

    /** adapted from https://prsinfo.clinicaltrials.gov/definitions.html */
    public static List<String> CLINICAL_PHASES = Arrays.asList(
            "Not Applicable", "Early Phase 1", "Phase 1", "Phase 1/Phase 2",
            "Phase 2", "Phase 2/Phase 3", "Phase 3", "Phase 4");

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
            String ei = e.id.toString();
            if (eventIndexes.containsKey(ei)) {
                eventIndexes.put(ei, eventIndexes.get(ei)+1);
                e.id = e.id + ":" + eventIndexes.get(ei);
            } else {
                eventIndexes.put(ei, 1);
            }
            props.put(ID, e.id);
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

        /** Development status: highest development phase attained
         * US Approved OTC
         * US Approved Rx
         * US Withdrawn / previously marketed [withdrawn]
         * US Unapproved, Currently Marketed
         * Marketed Outside US
         * Previously Marketed Outside US [withdrawn]
         * Investigational - Phase III, Phase II, Phase I, Clinical
         * Other
         */
        Event highestStatus = new Event("fake", stitch.getId(), Event.EventKind.Other);
        for (Event e: events) {
            if (e.kind != null) {
                if (highestStatus == null || e.kind.ordinal() < highestStatus.kind.ordinal())
                    highestStatus = e;
            }
        }

        /** Highest Phase - distinguish clinical phases, using highest CLINICAL_PHASES*/
        if (highestStatus.kind == Event.EventKind.Clinical) {
            for (int i=CLINICAL_PHASES.size()-2; i>-1; i--)
                for (Event e: events) {
                    if (e.kind == highestStatus.kind &&
                            CLINICAL_PHASES.indexOf(e.comment) > CLINICAL_PHASES.indexOf(highestStatus.comment))
                        highestStatus = e;
                }
//            if (highestStatus.comment != null) {
//                //System.out.println("Highest Phase: " + highestStatus.comment);
//                stitch.set(highestPhase, highestStatus.comment);
//            } else {
//                stitch.set(highestPhase, highestStatus.kind.name());
//            }
        }
//        else {
//            //System.out.println("Highest Status: " + highestStatus.kind);
//            stitch.set(highestPhase, highestStatus.kind.name());
//        }
        stitch.set(highestPhase, highestStatus == null ? "null" : highestStatus.id);

        // Calculate Initial Approval
        /** US Approval Year */
        /** Initial Approval Year */
        Event initUSAppr = null;
        Event initAppr = null;
        for (Event e: events) {
            if (e.startDate != null && "US".equals(e.jurisdiction) && e.kind.wasMarketed()) {
                if (initAppr == null || e.startDate.before(initAppr.startDate)) {
                    initAppr = e;
                }
                if (initUSAppr == null || e.startDate.before(initUSAppr.startDate)) {
                    initUSAppr = e;
                }
            }
            if (e.startDate != null && e.kind.wasMarketed()) {
                if (initAppr == null || e.startDate.before(initAppr.startDate)) {
                    initAppr = e;
                }
            }
        }
        stitch.set(initiallyMarketed, initAppr == null ? "null" : initAppr.id);
        stitch.set(initiallyMarketedUS, initUSAppr == null ? "null" : initUSAppr.id);

        Event approved = null;
        for (Event e: events) {
            if ("US".equals(e.jurisdiction) && e.kind.isApproved())
                approved = e;
        }
        stitch.set(USapproved, approved == null ? "null" : approved.id);

//        if (initUSAppr != null && initAppr != initUSAppr) {
//                cal.setTime(initUSAppr.startDate);
//                //System.out.println("Initial US marketing: " + cal.get(Calendar.YEAR));
//                stitch.set(initiallyMarketedUS, cal.get(Calendar.YEAR));
//        } else {
//            if (stitch.get(initiallyMarketedUS) != null)
//                stitch.removeProperty(initiallyMarketedUS);
//        }
//
//        if (initAppr != null) {
//            cal.setTime(initAppr.startDate);
//            //System.out.println("Initially marketed: "+cal.get(Calendar.YEAR) + " (" + initAppr.jurisdiction + ")");
//            if (initAppr.jurisdiction != null && initAppr.jurisdiction.indexOf("|") == -1)
//                stitch.set(initiallyMarketedJurisdiction, initAppr.jurisdiction);
//            else if (stitch.get(initiallyMarketedJurisdiction) != null)
//                stitch.removeProperty(initiallyMarketedJurisdiction);
//            stitch.set(initiallyMarketed, cal.get(Calendar.YEAR));
//        } else {
//            if (stitch.get(initiallyMarketed) != null)
//                stitch.removeProperty(initiallyMarketed);
//            if (stitch.get(initiallyMarketedJurisdiction) != null)
//                stitch.removeProperty(initiallyMarketedJurisdiction);
//        }

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
                //logger.info(ep.name + ": kind=" + e.kind
                //        + " startDate=" + e.startDate);
                events.add(e);
            }
            ep.reset();
        }

        return events;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+EventCalculator.class.getName()
                               +" DB VERSION [Stitch IDs]");
            System.exit(1);
        }

        EntityFactory ef = new EntityFactory (GraphDb.getInstance(argv[0]));
        EventCalculator ac = new EventCalculator(ef);
        int count;
        int ver = Integer.parseInt(argv[1]);
        if (ver < 3) {
            count = ac.recalculate(ver);
        } else {
            List<Long> nodeList = new ArrayList();
            for (int i=1; i<argv.length; i++) {
                String id = argv[i];
                Entity e = null;
                try {
                    long n = Long.parseLong(id);
                    e = ef.entity(n);
                    if (!e.is(AuxNodeType.SGROUP))
                        e = null;
                }
                catch (Exception ex) {
                    e = ef.entity(ver, id);
                }
                if (e != null) {
                    nodeList.add(e.getId());
                }
            }
            long[] nodes = new long[nodeList.size()];
            for (int i=0; i<nodeList.size(); i++)
                nodes[i] = nodeList.get(i);
            count = ac.recalculateNodes(nodes);
        }
        logger.info(count+" stitches recalculated!");
        ef.shutdown();
    }
}
