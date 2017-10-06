package ncats.stitcher.calculators;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import ncats.stitcher.Stitch;
import ncats.stitcher.DataSource;
import ncats.stitcher.EntityFactory;
import ncats.stitcher.DataSourceFactory;
import ncats.stitcher.AuxRelType;
import static ncats.stitcher.Props.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

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

        boolean approved = false, marketed = false;
        for (Approval a : approvals) {
            Map<String, Object> props = new HashMap<>();
            props.put(ID, a.id);
            DataSource source = dsf.getDataSourceByName(a.source);
            props.put(SOURCE, source.getKey());

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
            
            // now add event to this stitch node
            stitch.add(AuxRelType.EVENT.name(), props, data);
        }

        if (approved)
            stitch.addLabel("APPROVED");

        if (marketed)
            stitch.addLabel("MARKETED");            
    }

    static class Approval {
        public String source;
        public Object id;
        public Date approval;
        public Date marketed;

        public Approval (String source, Object id) {
            this (source, id, null, null);
        }

        public Approval (String source, Object id, Date approval, Date marketed) {
            this.source = source;
            this.id = id;
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
                            || approval.marketed.after(date))
                            approval = new Approval (name, id, null, date);
                    }
                    catch (Exception ex) {
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
                    for (int i = 0; i < Array.getLength(content); ++i) {
                        String s = (String) Array.get(content, i);
                        if (s.toLowerCase().indexOf("approved") >= 0) {
                            approval = new Approval (name, id);
                            break;
                        }
                    }
                } else if (((String)content).toLowerCase()
                           .indexOf("approved") >= 0) {
                    approval = new Approval (name, id);
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
                                    || approval.marketed.after(date))
                                    approval = new Approval (name, id, null, date);
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
                    for (int i = 0; i < Array.getLength(content); ++i) {
                        String s = (String) Array.get(content, i);
                        if (s.toLowerCase().indexOf("approved") >= 0) {
                            approval = new Approval (name, id);
                            break;
                        }
                    }
                }
                else if (((String)content)
                         .toLowerCase().indexOf("approved") >= 0) 
                    approval = new Approval (name, id);
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
                    || approval.approval.after(date))
                    approval = new Approval (name, id, null, date);
            }

            content = payload.get("MarketDate");
            if (content != null) {
                try {
                    Date date = SDF.parse((String)content);
                    if (approval == null)
                        approval = new Approval (name, id, null, date);
                    else if (approval.marketed.after(date)) {
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
            if (stitch.is(ap.name)) {
                Map<String, Object> payload = stitch.payload(ap.name);
                if (payload != null) {
                    Approval a = ap.getApproval(payload);
                    if (a == null) {
                    }
                    else {
                        logger.info(ap.name+": approved="+a.approval
                                    +" marketed="+a.marketed);
                        approvals.add(a);
                    }
                }
            }
        }

        return approvals;
    }
}
