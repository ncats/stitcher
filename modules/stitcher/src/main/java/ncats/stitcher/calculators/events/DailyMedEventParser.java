package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DailyMedEventParser extends EventParser {
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

    private static class DevelopmentStatusLookup{
        //MarketingStatus	ProductType	DevelopmentStatus



        private final Map<String, Map<String, Map<String, DevelopmentStatus>>> map;

        private DevelopmentStatusLookup(Map<String, Map<String, Map<String, DevelopmentStatus>>> map) {
            this.map = map;
        }
        private static Pattern DELIM = Pattern.compile("\t");
        public static DevelopmentStatusLookup parse(InputStream in) throws IOException{
            Objects.requireNonNull( in, "inputstream not found!");
            Map<String, Map<String, Map<String, DevelopmentStatus>>> map = new HashMap<>();

            try(BufferedReader reader = new BufferedReader(new InputStreamReader(in))){

                String line;
                while ((line = reader.readLine()) !=null){
                    String[] cols = DELIM.split(line);
                    
                    map.computeIfAbsent(cols[0], k-> new HashMap<>())
                            .computeIfAbsent(cols[1], l-> new HashMap<>())
                            .put(cols[2], DevelopmentStatus.parse(cols[3]));
                }
            }

            return new DevelopmentStatusLookup(map);
        }

        public Event.EventKind lookup(String marketingStatus, String productType, String splComment){
            Map<String, Map<String, DevelopmentStatus>> subMap = map.get(marketingStatus);
            if(subMap !=null){
                Map<String, DevelopmentStatus> subMap2 = subMap.get(productType);
                if(subMap2 !=null){
                    DevelopmentStatus status = subMap2.get(splComment);
                    if(status !=null){
                        return status.getKind();
                    }
                }
            }
            return null;
        }
    }
    public DailyMedEventParser(String source) {
        super (source);
    }

    /**
     * Code of Federal Regulations Title 21
     *
     * looks like anything in 3XX is for human consumption
     */
    private static Pattern CFR_21_OTC_PATTERN =
        Pattern.compile("part3([1-9][0-9]).*");

    static DevelopmentStatusLookup developmentStatusLookup;


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
            Object marketStatus = payload.get("MarketingStatus");

            boolean isVeterinaryProduct=false;
            if(marketStatus !=null){
                isVeterinaryProduct = ((String) marketStatus).equalsIgnoreCase("bulk ingredient for animal drug compounding");
            }

            content = payload.get("InitialYearApproval");
            if (content != null) {
                date = EventCalculator.SDF.parse((String) content + "-12-31");
            }

            content = payload.get("MarketDate");
            if (content != null) {
                Date date2 = EventCalculator.SDF.parse((String)content);
                if (date == null || date.after(date2))
                    date = date2;
            }
            //exclude veterinary products from approved and/or Marketed
            if (!isVeterinaryProduct && date != null) {

                String productType = (String) payload.get("ProductCategory");
                String splComment = (String) payload.get("Comment");
                Event.EventKind et=null;
                
                //ProductType shouldn't ever be null
                if(productType != null){
                    //some fields may be null, so set the corresponding vars to empty strings for correct lookup 
                    if(marketStatus == null){
                        marketStatus = "";
                    }
                    if(splComment == null){
                        splComment = "";
                    }                    
                    et = developmentStatusLookup.lookup((String) marketStatus, productType, splComment);
                }

                if(et ==null) {
                    //use old logic
                    String UNII = (String) payload.get("UNII");
                    System.out.println(UNII);
                    System.out.println("FALLING BACK TO OLD LOGIC!!!");

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
                                EventCalculator.logger.log(Level.WARNING,
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
                event.comment = (String)(content==null? "": content);

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
            EventCalculator.logger.log(Level.SEVERE, "Can't parse date: "+content, ex);
        }

        if (event != null) events.add(event);
        return events;
    }
}
