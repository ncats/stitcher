package ncats.stitcher.calculators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ncats.stitcher.AuxRelType;
import ncats.stitcher.DataSourceFactory;
import ncats.stitcher.EntityFactory;
import ncats.stitcher.Stitch;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static ncats.stitcher.Props.*;

public class ApprovalCalculator implements StitchCalculator {
    static final Logger logger = Logger.getLogger(Stitch.class.getName());
    static final SimpleDateFormat SDF = new SimpleDateFormat ("yyyy-MM-dd");

    final DataSourceFactory dsf;
    public ApprovalCalculator (EntityFactory ef) {
        this.dsf = ef.getDataSourceFactory();
    }

    /*
     * Consumer<Stitch>
     */
    public void accept (Stitch stitch) {
        List<Approval> approvals = getApprovalEvents (stitch);
        logger.info("Stitch "+stitch.getId()+" => "+approvals.size()+
                    " approval events");

        stitch.removeAll(AuxRelType.EVENT.name());
        Set<String> labels = new TreeSet<>();
        boolean approved = false, marketed = false;
        for (Approval a : approvals) {
            Map<String, Object> props = new HashMap<>();
            props.put(ID, a.id);
            props.put(SOURCE, a.source);
            props.put(KIND, "Regulatory Status");

            Map<String, Object> data = new HashMap<>();
            if (a.approval != null) {
                approved = true;
                Calendar cal = Calendar.getInstance();
                cal.setTime(a.approval);
                data.put("approvalYear", cal.get(Calendar.YEAR));
            }

            if (a.marketed != null) {
                data.put("marketedDate", SDF.format(a.marketed));
                marketed = true;
            }

            if (a.comment != null)
                data.put("comment", a.comment);
            
            labels.add(a.source);
            
            // now add event to this stitch node
            stitch.add(AuxRelType.EVENT.name(), props, data);
        }

        if (approved)
            labels.add("APPROVED");

        if (marketed)
            labels.add("MARKETED");

        stitch.addLabel(labels.toArray(new String[0]));
    }

    static class Approval {
        public String source;
        public Object id;
        public Date approval;
        public Date marketed;
        public String comment;

        public Approval (String source, Object id) {
            this (source, id, null, null);
        }

        public Approval (String source, Object id,
                         Date approval, Date marketed) {
            this.source = source;
            this.id = id != null ? id : "*";
            this.approval = approval;
            this.marketed = marketed;
        }
    }
        
    static abstract class ApprovalParser {
        final public String name;
        protected ApprovalParser (String name) {
            this.name = name;
        }

        public abstract Approval getApproval (Map<String, Object> payload);
    }

    static class RanchoApprovalParser extends ApprovalParser {
        final ObjectMapper mapper = new ObjectMapper ();
        final Base64.Decoder decoder = Base64.getDecoder();
        Approval approval;
        Object id;

        public RanchoApprovalParser () {
            super ("rancho_2017-09-07_20-32.json");
        }

        void parseApproval (JsonNode n) {
            if (n.has("HighestPhase") && "approved".equalsIgnoreCase
                (n.get("HighestPhase").asText())) {
                if (n.has("ConditionProductDate")) {
                    String d = n.get("ConditionProductDate").asText();
                    try {
                        Date date = SDF.parse(d);
                        if (approval == null || approval.marketed == null
                            || approval.marketed.after(date)) {
                            approval = new Approval (name, id, null, date);
                            if (n.has("ConditionName"))
                                approval.comment =
                                    n.get("ConditionName").asText();
                            else if (n.has("ConditionMeshValue"))
                                approval.comment =
                                    n.get("ConditionMeshValue").asText();
                        }
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                        logger.log(Level.SEVERE,
                                   "Can't parse date: "+d, ex);
                    }
                }
                else {
                    logger.warning("Approved Condition without date!");
                }
            }
        }

        void parseApproval (String content) throws Exception {
            JsonNode node = mapper.readTree(decoder.decode(content));
            if (node.isArray()) {
                for (int i = 0; i < node.size(); ++i) {
                    JsonNode n = node.get(i);
                    parseApproval (n);
                }
            }
            else 
                parseApproval (node);
        }

        public Approval getApproval (Map<String, Object> payload) {
            approval = null;
            id = payload.get("Unii");

            Object content = payload.get("Conditions");
            if (content != null) {
                if (content.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(content); ++i) {
                        String c = (String) Array.get(content, i);
                        try {
                            parseApproval (c);
                        }
                        catch (Exception ex) {
                            logger.log(Level.SEVERE, 
                                       "Can't parse Json payload: "+c, ex);
                        }
                    }
                }
                else {
                    try {
                        parseApproval ((String)content);
                    }
                    catch (Exception ex) {
                        logger.log(Level.SEVERE, 
                                   "Can't parse json payload: "+content, ex);
                    }
                }
            }

            return approval;
        }
    }
        
    static class NPCApprovalParser extends ApprovalParser {
        public NPCApprovalParser () {
            super ("npc-dump-1.2-04-25-2012_annot.sdf.gz");
        }
        
        public Approval getApproval (Map<String, Object> payload) {
            Approval approval = null;
            Object content = payload.get("DATASET");
            Object id = payload.get("ID");

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
                        approval = new Approval (name, id);
                        approval.comment = approved.toString();
                    }
                }
                else if (((String)content).toLowerCase()
                           .indexOf("approved") >= 0) {
                    approval = new Approval (name, id);
                    approval.comment = (String)content;
                }
            }
            
            return approval;
        }
    }
    
    static class PharmManuApprovalParser extends ApprovalParser {
        final ObjectMapper mapper = new ObjectMapper ();
        final Base64.Decoder decoder = Base64.getDecoder();
            
        public PharmManuApprovalParser () {
            super ("PharmManuEncycl3rdEd.json");
        }
            
        public Approval getApproval (Map<String, Object> payload) {
            Approval approval = null;
            Object id = payload.get("UNII");

            String content = (String) payload.get("Drug Products");
            if (content != null) {
                try {
                    for (String c : content.split("\n")) {
                        JsonNode node = mapper.readTree(decoder.decode(c));
                        if (node.has("Year Introduced")) {
                            String year = node.get("Year Introduced").asText();
                            try {
                                Calendar cal = Calendar.getInstance();
                                cal.set(Calendar.YEAR, 
                                        Integer.parseInt(year));
                                Date date = cal.getTime();
                                if (approval == null 
                                    || approval.marketed == null
                                    || approval.marketed.after(date)) {
                                    approval = new Approval
                                        (name, id, null, date);
                                    if (node.has("Country"))
                                        approval.comment =
                                            node.get("Country").asText();
                                }
                            }
                            catch (Exception ex) {
                                logger.log(Level.SEVERE, 
                                           "Can't parse date: "+year, ex);
                            }
                        }
                    }
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, 
                               "Can't parse json: '"+content+"'", ex);
                }
            }
                
            return approval;
        }
    }
        
    static class DrugBankApprovalParser extends ApprovalParser {
        public DrugBankApprovalParser () {
            super ("drugbank-full-annotated.sdf");
        }
            
        public Approval getApproval (Map<String, Object> payload) {
            Approval approval = null;
            Object id = payload.get("DATABASE_ID");
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
                        approval = new Approval (name, id);
                        approval.comment = approved.toString();
                    }
                }
                else if (((String)content)
                         .toLowerCase().indexOf("approved") >= 0) {
                    approval = new Approval (name, id);
                    approval.comment = (String)content;
                }
            }
            return approval;
        }
    }

    static class DailyMedApprovalParser extends ApprovalParser {
        public DailyMedApprovalParser () {
            super ("spl_acti_rx.txt");
        }

        public Approval getApproval (Map<String, Object> payload) {
            Approval approval = null;
            Object id = payload.get("NDC");
            
            Object content = payload.get("InitialYearApproval");
            if (content != null) {
                Calendar cal = Calendar.getInstance();
                cal.set(Calendar.YEAR, Integer.parseInt(content.toString()));
                Date date = cal.getTime();
                if (approval == null 
                    || approval.approval == null
                    || approval.approval.after(date)) {
                    approval = new Approval (name, id, date, null);
                    approval.comment = (String)payload.get("ApprovalAppId");
                }
            }

            content = payload.get("MarketDate");
            if (content != null) {
                try {
                    Date date = SDF.parse((String)content);
                    if (approval == null) {
                        approval = new Approval (name, id, null, date);
                        approval.comment = (String)payload.get("ApprovalAppId");
                    }
                    else if (approval.marketed == null
                             || approval.marketed.after(date)) {
                        approval.marketed = date;
                    }
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't parse date: "+content, ex);
                }
            }

            return approval;
        }
    }
        
    static ApprovalParser[] ApprovalParsers = {
        new RanchoApprovalParser (),
        new NPCApprovalParser (),
        new PharmManuApprovalParser (),
        new DrugBankApprovalParser (),
        new DailyMedApprovalParser ()
    };

    List<Approval> getApprovalEvents (Stitch stitch) {
        List<Approval> approvals = new ArrayList<>();

        for (ApprovalParser ap : ApprovalParsers) {
            try {
                Map<String, Object> payload = stitch.payload(ap.name);
                if (payload != null) {
                    Approval a = ap.getApproval(payload);
                    if (a == null) {
                    } else {
                        logger.info(ap.name + ": approved=" + a.approval
                                + " marketed=" + a.marketed);
                        approvals.add(a);
                    }
                }
            } catch (IllegalArgumentException iae) {
                logger.warning(ap.name + " not a valid data source");
            }
        }

        return approvals;
    }
}
