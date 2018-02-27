package ncats.stitcher.calculators;

import ncats.stitcher.*;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import ncats.stitcher.calculators.events.*;

import static ncats.stitcher.Props.*;

public class EventCalculator implements StitchCalculator {

    static EventParser[] DEFAULT_EVENT_PARSERS = {
            new RanchoEventParser(),
            new NPCEventParser (),
            new PharmManuEventParser (),
            //new DrugBankEventParser (),
            new DrugBankXmlEventParser (),
            new DailyMedRxEventParser (),
            new DailyMedOtcEventParser (),
            new DailyMedRemEventParser (),
            new DrugsAtFDAEventParser (),

            new WithdrawnEventParser()
    };


    public static final Logger logger = Logger.getLogger(Stitch.class.getName());
    public static final SimpleDateFormat SDF = new SimpleDateFormat ("yyyy-MM-dd");
    public static final SimpleDateFormat SDF2 = new SimpleDateFormat ("MM/dd/yyyy");

    final EntityFactory ef;
    final DataSourceFactory dsf;


    private List<EventParser> eventParsers = Arrays.asList(DEFAULT_EVENT_PARSERS);

    public EventCalculator(EntityFactory ef) {
        this.ef = ef;
        this.dsf = ef.getDataSourceFactory();
    }

    public void setEventParsers(List<EventParser> parsers){
        this.eventParsers = new ArrayList<>(Objects.requireNonNull(parsers));
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
            props.put(SOURCE, e.source);
            props.put(KIND, e.kind.toString());

            labels.add(e.kind);

            Map<String, Object> data = new HashMap<>();
            if (e.date != null)
                data.put("date", SDF.format(e.date));

            if (e.jurisdiction != null)
                data.put("jurisdiction", e.jurisdiction);

            if (e.comment != null)
                data.put("comment", e.comment);

            if(e.route !=null){
                data.put("route", e.route);
            }

            if(e.approvalAppId !=null){
                data.put("ApprovalAppId", e.approvalAppId);
            }
            if(e.marketingStatus !=null){
                data.put("MarketingStatus", e.marketingStatus);
            }
            if(e.NDC !=null){
                data.put("NDC", e.NDC);
            }
            if(e.URL !=null){
                data.put("URL", e.URL);
            }
            if(e.withDrawnYear !=null){
                data.put("withdrawn_year", e.withDrawnYear);
            }

            labels.add(e.source);
            if (e.date != null && e.kind.isApproved()) {
                cal.setTime(e.date);
                approvals.put(e.source, cal.get(Calendar.YEAR));
            }
            
            // now add event to this stitch node
            stitch.addIfAbsent(AuxRelType.EVENT.name(), props, data);
        }

        stitch.set("approved", labels.isApproved);
        
        // sources that have approval dates; order by relevance
        for (EventParser ep : new EventParser[]{
                new DrugsAtFDAEventParser(),
                new DailyMedRxEventParser(),
                new DailyMedOtcEventParser(),
                new DailyMedRemEventParser(),
                new WithdrawnEventParser()
                //new DrugBankXmlEventParser()
            }) {
            Integer year = approvals.get(ep.name);
            if (year != null) {
                stitch.set("approvedYear", year);
                break;
            }
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


    List<Event> getEvents (Stitch stitch) {
        List<Event> events = new ArrayList<>();

        for (EventParser ep : eventParsers) {
            try {
                for(Map<String, Object> payload : stitch.multiplePayloads(ep.name)){
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
