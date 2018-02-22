package ncats.stitcher.calculators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ncats.stitcher.*;

import java.io.*;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.*;
import javax.xml.stream.events.*;
import javax.xml.namespace.QName;
import javax.xml.parsers.*;
import org.w3c.dom.*;

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
    static class Event implements Cloneable{
        public EventKind kind;
        public String source;
        public Object id;
        public Date date;
        public String jurisdiction;
        public String comment; // reference

        public String route;

        public String approvalAppId;

        public String marketingStatus;
        public String productCategory;

        public String NDC;
        public String URL;

        /**
         * Create a deep clone.
         * @return
         * @throws CloneNotSupportedException
         */
        @Override
        public Event clone()  {
            Event e = null;
            try {
                e = (Event) super.clone();
            } catch (CloneNotSupportedException e1) {
                throw new RuntimeException(e1); //shouldn't happen
            }
            //date is mutable so make defensive copy
            e.date = new Date(date.getTime());
            return e;
        }

        public enum EventKind {
            Publication,
            Filing,
            Designation,
            ApprovalRx {
                @Override
                public boolean isApproved() {
                    return true;
                }
            },
            Marketed,
            ApprovalOTC {
                @Override
                public boolean isApproved(){
                    return true;
                }
            },
            Other,
            Withdrawn
            ;

            public boolean isApproved(){
                return false;
            }
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
            super ("rancho-export_2017-10-26_20-39.json");
        }

        void parseEvent (JsonNode n) {
            if (n.has("HighestPhase") && ("approved".equalsIgnoreCase
                (n.get("HighestPhase").asText())) || ("phase IV".equalsIgnoreCase
                    (n.get("HighestPhase").asText()))) {
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
                        event.comment += " "
                            + n.get("ConditionProductName").asText();
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

                if(n.has("InVivoUseRoute")){
                    String route = n.get("InVivoUseRoute").asText();
                    //InVivoUseRoute is a controlled vocabularly
                    //if the value = "other" than use the field InVivoUseRouteOther
                    //which is free text.
                    if("other".equalsIgnoreCase(route)){
                        route = n.get("InVivoUseRouteOther").asText();
                    }
                    if(route !=null && !route.trim().isEmpty()){
                        event.route = route;
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

    static class DrugBankXmlEventParser extends EventParser {
        protected String id;
        public DrugBankXmlEventParser () {
            super ("drugbank_all_full_database.xml.zip");
        }

        public List<Event> getEvents(Map<String, Object> payload) {
            List<Event> events = new ArrayList<>();
            String xml = (String) payload.get("xml");
            id = (String) payload.get("drugbank-id");
            if (xml != null) { // unlikely!
                try {
                    parseEvents (events, xml);
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't parse xml document for "
                               +payload.get("drugbank-id"), ex);
                }
            }
            return events;
        }

        void parseEvents (List<Event> events, String xml) throws Exception {
            DocumentBuilder builder =
                DocumentBuilderFactory.newInstance().newDocumentBuilder();
            xml = Util.decode64(xml, true);
            try {
                Element doc = builder.parse
                    (new ByteArrayInputStream (xml.getBytes("utf8")))
                    .getDocumentElement();
                NodeList products = doc.getElementsByTagName("product");
                for (int i = 0; i < products.getLength(); ++i) {
                    Element p = (Element)products.item(i);
                    Event ev = parseEvent (p);
                    if (ev != null)
                        events.add(ev);
                }
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Bogus XML: "+xml, ex);
            }
        }

        Event parseEvent (Element p) {
            NodeList children = p.getChildNodes();
            Event ev = new Event (name, id, Event.EventKind.Marketed);
            for (int i = 0; i < children.getLength(); ++i) {
                Node child = children.item(i);
                if ( Node.ELEMENT_NODE == child.getNodeType()) {
                    Element n = (Element)child;
                    
                    boolean approved = false;
                    Map<String, String> map = new HashMap<>();
                    switch (n.getTagName()) {
                    case "approved":
                        approved = "true".equalsIgnoreCase(n.getTextContent());
                        break;
                        
                    case "over-the-counter":
                        if ("true".equalsIgnoreCase(n.getTextContent()))
                            ev.kind = Event.EventKind.ApprovalOTC;
                        break;
                        
                    case "country":
                        ev.jurisdiction = n.getTextContent();
                        break;
                        
                    case "started-marketing-on":
                        try {
                            ev.date = SDF.parse(n.getTextContent());
                        }
                        catch (Exception ex) {
                            logger.warning("Bogus date format: "
                                           +n.getTextContent());
                        }
                        break;
                        
                    default:
                        map.put(n.getTagName(), n.getTextContent());
                    }
                    
                    if (!approved) {
                        ev.kind = Event.EventKind.Marketed;
                    }
                    ev.comment = map.get("name")+" ["+map.get("labeller")+"]";
                }
            }
            return ev;
        }
    }

    private enum DevelopmentStatus{
/*
1 DevelopmentStatus
  63 Other
  12 US Approved OTC
  16 US Approved Rx
  38 US Unapproved, Marketed
 */

        Other("Other", Event.EventKind.Other),
        US_Approved_OTC("US Approved OTC", Event.EventKind.ApprovalOTC),
        US_Approved_Rx("US Approved Rx", Event.EventKind.ApprovalRx),
        US_Unapproved_Marketed("US Unapproved, Marketed", Event.EventKind.Marketed)
        ;
        private static Map<String, DevelopmentStatus> map = new HashMap<>();

        private String displayName;

        private Event.EventKind kind;

        static{
            for(DevelopmentStatus s : values()){
                map.put(s.displayName, s);
            }
        }
        DevelopmentStatus(String displayName, Event.EventKind kind){
            this.displayName = displayName;
            this.kind = kind;
        }

        public String getDisplayName(){
            return displayName;
        }

        public static DevelopmentStatus parse(String displayName){
            return map.get(displayName);
        }

        public Event.EventKind getKind(){
            return kind;
        }

    }


    private static class WithdrawnEventParser extends EventParser{
        public WithdrawnEventParser() {
            super("combined_withdrawn_shortage_drugs.txt");
        }

        private static Pattern COUNTRY_DELIM = Pattern.compile("\\|");
        @Override
        public List<Event> getEvents(Map<String, Object> payload) {
            List<Event> events = new ArrayList<>();
            String unii = (String) payload.get("UNII");
            if("Withdrawn".equals(payload.get("status"))){
                Event e = new Event(name, unii, Event.EventKind.Withdrawn);
                e.comment = (String) payload.get("reason_withdrawn");
                String countries = (String) payload.get("country_withdrawn");
                if(countries == null) {
                    events.add(e);
                }else{
                    //lists of countries are pipe delimited
                    String[] list = COUNTRY_DELIM.split(countries);
                    for(String country : list){
                        Event copy = e.clone();
                        copy.jurisdiction = country;
                        events.add(copy);
                    }
                }

            }
            return events;
        }
    }
    private static class DevelopmentStatusLookup{
        //MarketingStatus	ProductType	DevelopmentStatus



        private final Map<String, Map<String, DevelopmentStatus>> map;

        private DevelopmentStatusLookup(Map<String, Map<String, DevelopmentStatus>> map) {
            this.map = map;;
        }
        private static Pattern DELIM = Pattern.compile("\t");
        public static DevelopmentStatusLookup parse(InputStream in) throws IOException{
            Objects.requireNonNull( in, "inputstream not found!");
            Map<String, Map<String, DevelopmentStatus>> map = new HashMap<>();
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(in))){

                String line;
                while ((line = reader.readLine()) !=null){
                    String[] cols = DELIM.split(line);

                    map.computeIfAbsent(cols[0], k-> new HashMap<>())
                            .put(cols[1], DevelopmentStatus.parse(cols[2]));
                }
            }

            return new DevelopmentStatusLookup(map);
        }

        public Event.EventKind lookup(String marketingStatus, String productType){
            Map<String, DevelopmentStatus> subMap = map.get(marketingStatus);
            if(subMap !=null){
                DevelopmentStatus status = subMap.get(productType);
                if(status !=null){
                    return status.getKind();
                }
            }

            return null;
        }
    }

    static class DailyMedEventParser extends EventParser {
        public DailyMedEventParser (String source) {
            super (source);
        }

        /**
         * Code of Federal Regulations Title 21
         *
         * looks like anything in 3XX is for human consumption
         */
        private static Pattern CFR_21_OTC_PATTERN =
            Pattern.compile("part3([1-9][0-9]).*");

        private static DevelopmentStatusLookup developmentStatusLookup;

        static{
            try{
                developmentStatusLookup = DevelopmentStatusLookup.parse(new BufferedInputStream(new FileInputStream("data/dev_status_logic.txt")));
            }catch(IOException e){
                throw new UncheckedIOException(e);
            }
        }

        public List<Event> getEvents(Map<String, Object> payload) {
            List<Event> events = new ArrayList<>();
            Event event = null;
            Object id = payload.get("NDC");

            Object content = null;
            Date date = null;



            try {


                //Jan 2018 new columns for Route and MarketStatus
                Object marketStatus = payload.get("MarketStatus");

                boolean isVeterinaryProduct=false;
                if(marketStatus !=null){
                    isVeterinaryProduct = ((String) marketStatus).equalsIgnoreCase("bulk ingredient for animal drug compounding");
                }

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
                //exclude veterinary products from approved and/or Marketed
                if (!isVeterinaryProduct && date != null) {

                    String productType = (String) payload.get("ProductCategory");
                    Event.EventKind et=null;
                    if(marketStatus !=null && productType !=null ){
                        et = developmentStatusLookup.lookup((String) marketStatus, productType);
                    }
                    if(et ==null) {
                        //use old logic
                        et = Event.EventKind.Marketed;
                        content = payload.get("ApprovalAppId");
                        if (content != null) {
                            String contentStr = (String) content;
                            if (contentStr.startsWith("NDA") ||
                                    contentStr.startsWith("ANDA") ||
                                    contentStr.startsWith("BA") ||
                                    contentStr.startsWith("BN") ||
                                    contentStr.startsWith("BLA")) {
                                et = Event.EventKind.ApprovalRx;
                            } else {
                                Matcher matcher = CFR_21_OTC_PATTERN.matcher(contentStr);
                                if (matcher.find()) {
                                    //We include if it's 310.545. We exclude if it's any other 310.5XX
                                    if (contentStr.startsWith("310.5")) {
                                        if (contentStr.equals("310.545")) {
                                            et = Event.EventKind.ApprovalOTC;
                                        }

                                    } else {
                                        et = Event.EventKind.ApprovalOTC;
                                    }
                                } else {
                                    logger.log(Level.WARNING,
                                            "Unusual approval app id: " + content
                                                    + ": " + payload.toString());
                                }
                            }
                        }
                    }

                    event = new Event(name, id, et);
                    //if market Status is Export Only  remove Us jurisdiction
                    if(marketStatus !=null && marketStatus instanceof String
                            && ((String)(String) marketStatus).equalsIgnoreCase("export only")){
                        event.jurisdiction = null;
                    }else {
                        event.jurisdiction = "US";
                    }
                    event.date = date;
                    event.comment = (String)content;

                    event.approvalAppId = (String) payload.get("ApprovalAppId");

                    event.marketingStatus = (String) marketStatus;

                    event.productCategory = (String) payload.get("ProductCategory");

                    event.NDC = (String) payload.get("NDC");
                    event.URL = (String) payload.get("URL");
                    //Jan 2018 new columns for Route and MarketStatus
                    Object route = payload.get("Route");
                    if(route !=null){
                        event.route = (String) route;
                    }
                }


            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't parse date: "+content, ex);
            }

            if (event != null) events.add(event);
            return events;
        }
    }

    static class DailyMedRxEventParser extends DailyMedEventParser {
        public DailyMedRxEventParser () {
            super ("spl_acti_rx.txt");
        }
    }

    static class DailyMedOtcEventParser extends DailyMedEventParser {
        public DailyMedOtcEventParser () {
            super ("spl_acti_otc.txt");
        }
    }

    static class DailyMedRemEventParser extends DailyMedEventParser {
        public DailyMedRemEventParser () {
            super ("spl_acti_rem.txt");
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
                        || (event.kind.isApproved()
                            && event.date.after(date))) {
                        event = new Event(name, id, Event.EventKind.ApprovalRx);
                        event.date = date;
                        event.jurisdiction = "US";
                        event.comment = (String) payload.get("Comment");
                    }
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE,
                               "Can't parse date: \""+content+"\"", ex);
                }
            }

            if (event != null) events.add(event);
            return events;
        }       
    }

    static EventParser[] eventParsers = {
        new RanchoEventParser (),
        new NPCEventParser (),
        new PharmManuEventParser (),
        //new DrugBankEventParser (),
        new DrugBankXmlEventParser (),
        new DailyMedRxEventParser (),
        new DailyMedOtcEventParser (),
        new DailyMedRemEventParser (),
        new DrugsAtFDAEventParser ()
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
