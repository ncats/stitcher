package ncats.stitcher.calculators.events;

import ncats.stitcher.Util;
import ncats.stitcher.calculators.EventCalculator;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.util.*;
import java.util.logging.Level;

public class DrugBankXmlEventParser extends EventParser {
    protected String id;
    public DrugBankXmlEventParser() {
        super ("drugbank_all_full_database.xml.zip");
    }

    public void produceEvents(Map<String, Object> payload) {
        String xml = (String) payload.get("xml");
        id = (String) payload.get("drugbank-id");
        if (xml != null) { // unlikely!
            try {
                parseEvents (events, xml);
            }
            catch (Exception ex) {
                EventCalculator.logger.log(Level.SEVERE, "Can't parse xml document for "
                           +payload.get("drugbank-id"), ex);
            }
        }
        return;
    }

    void parseEvents (Map<String, Event> events, String xml) throws Exception {
        DocumentBuilder builder =
            DocumentBuilderFactory.newInstance().newDocumentBuilder();
        xml = Util.decode64(xml, true);
        try {
            Element doc = builder.parse
                (new ByteArrayInputStream(xml.getBytes("utf8")))
                .getDocumentElement();
            NodeList products = doc.getElementsByTagName("product");
            for (int i = 0; i < products.getLength(); ++i) {
                Element p = (Element) products.item(i);
                Event ev = parseEvent(p);
                if (ev != null) {
                    String key = ev.jurisdiction + ":" + ev.route;
                    events.put(key + "|" + System.identityHashCode(ev), ev);
                }
            }
            List<String> remove = new ArrayList();
            for (String key1: events.keySet()) {
                if (!remove.contains(key1))
                for (String key2 : events.keySet()) {
                    if (!remove.contains(key2) && !key1.equals(key2) &&
                            key1.substring(0,key1.indexOf("|")).
                                    equals(key2.substring(0,key2.indexOf("|")))) {
                        Event ev1 = events.get(key1);
                        Event ev2 = events.get(key2);
                        if (ev1.startDate != null && ev2.startDate != null)
                            if (!ev2.startDate.before(ev1.startDate)) {
                                if (ev1.endDate == null ||
                                        (ev2.endDate != null && !ev2.endDate.after(ev1.endDate))) {
                                    remove.add(key2);
                                }
                            }
                    }
                }
            }
            for (String key: remove)
                events.remove(key);
        }
        catch (Exception ex) {
            EventCalculator.logger.log(Level.SEVERE, "Bogus XML: "+xml, ex);
        }
    }

    Map<String, String> eventElems = new HashMap();
    {
        eventElems.put("ema-product-code", "approvalAppId");
        eventElems.put("ndc-product-code", "comment");
        eventElems.put("ema-ma-number", "approvalAppId");
        eventElems.put("fda-application-number", "approvalAppId");
        eventElems.put("country", "jurisdiction");
        eventElems.put("ended-marketing-on", "endDate");
        eventElems.put("started-marketing-on", "startDate");
        eventElems.put("name", "product");
        eventElems.put("labeller", "sponsor");
        eventElems.put("route", "route");
        eventElems.put("approved", "apprCheck");
        eventElems.put("generic", "generic");
        eventElems.put("over-the-counter", "otc");
        eventElems.put("source", "source"); // FDA NDC, DPD
    }

    //Event event = new Event(name, id, Event.EventKind.Marketed);
    //event.source = name;
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

    Event parseEvent (Element p) {
        Map<String, String> vals = new HashMap();
        NodeList children = p.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node child = children.item(i);
            if (Node.ELEMENT_NODE == child.getNodeType()) {
                Element n = (Element) child;
                String key = n.getTagName();
                String val = n.getTextContent();
                if (val != null && !val.trim().isEmpty())
                    if (eventElems.containsKey(key))
                        vals.put(eventElems.get(key), val);
                    else vals.put(n.getTagName(), val);
            }
        }
        Event ev = new Event(name, id, Event.EventKind.Marketed);
        for (Field field : ev.getClass().getDeclaredFields())
            if (vals.containsKey(field.getName()))
                try {
                    if (field.getType() == String.class)
                        field.set(ev, vals.get(field.getName()));
                    if (field.getType() == Date.class) {
                        field.set(ev, EventCalculator.SDF.parse(vals.get(field.getName())));
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

        ev.URL = "https://www.drugbank.ca/drugs/"+id;

        // handle apprCheck, generic, otc info
        if (vals.containsKey("endDate"))
            ev.kind = Event.EventKind.Discontinued;
        else if (vals.containsKey("apprCheck") && vals.get("apprCheck").
                equalsIgnoreCase("false"))
            ev.kind = Event.EventKind.Marketed;
        else if (ev.jurisdiction.equals("US")) {
            ev = null;
//            if (vals.containsKey("otc") && vals.get("otc").
//                    equalsIgnoreCase("true")) {
//                /** It doesnt appear that DrugBank can be trusted for approval status
//                 * DrugBank gets its product info via FDA NDC and NOT from drugs@FDA
//                 * For example, a product can assert that it follows the monograph, but still list active ingredients
//                 * that aren't listed - https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?id=58407
//                 * Adenosine should not be considered an approved OTC drug based on that label, for example*/
//                //ev.kind = Event.EventKind.USApprovalOTC;
//                ev.kind = Event.EventKind.USUnapproved;
//            } else if (vals.containsKey("apprCheck") && vals.get("apprCheck").
//                    equalsIgnoreCase("true")) {
//                /** It doesnt appear that DrugBank doesn't handle subtleties in products like allergenic extracts either
//                 * NDC 36987-1775 BLA102192*/
//                ev.kind = Event.EventKind.USUnapproved;
//                String contentStr = ev.approvalAppId;
//                if (!ev.source.equals("FDA NDC")) // FDA NDC is the ONLY US source for DrugBank - this will never happen
//                    ev.kind = Event.EventKind.USApprovalRx;
//                if (contentStr == null)
//                    ev.kind = Event.EventKind.USUnapproved;
//                else if (contentStr.startsWith("NDA") ||
//                        contentStr.startsWith("ANDA") ||
//                        contentStr.startsWith("BA") ||
//                        contentStr.startsWith("BN") ||
//                        contentStr.startsWith("BLA")) {
//                    ev.kind = Event.EventKind.USApprovalRx;
//                }
//            }
        }

//        if (!id.isEmpty())
//            return ev;
//        for (int i = 0; i < children.getLength(); ++i) {
//            Node child = children.item(i);
//            if ( Node.ELEMENT_NODE == child.getNodeType()) {
//                Element n = (Element)child;
//
//                boolean approved = false;
//                Map<String, String> map = new HashMap<>();
//                switch (n.getTagName()) {
//                case "approved":
//                    approved = "true".equalsIgnoreCase(n.getTextContent());
//                    break;
//
//                case "over-the-counter":
//                    if ("true".equalsIgnoreCase(n.getTextContent()))
//                        ev.kind = Event.EventKind.USApprovalOTC;
//                    break;
//
//                case "country":
//                    ev.jurisdiction = n.getTextContent();
//                    break;
//
//                case "started-marketing-on":
//                    try {
//                        ev.startDate = EventCalculator.SDF.parse(n.getTextContent());
//                    }
//                    catch (Exception ex) {
//                        EventCalculator.logger.warning("Bogus startDate format: "
//                                       +n.getTextContent());
//                    }
//                    break;
//
//                default:
//                    map.put(n.getTagName(), n.getTextContent());
//                }
//
//                if (!approved) {
//                    ev.kind = Event.EventKind.Marketed;
//                }
//                //only inclue comment if it's present
//                String name = map.get("name");
//                if(name !=null){
//                    StringBuilder comment = new StringBuilder(name);
//
//                    String labeller = map.get("labeller");
//                    if(labeller !=null){
//                        comment.append(" [").append(labeller).append("]");
//                    }
//                    ev.comment = comment.toString();
//                }
////                    ev.comment = map.get("name")+" ["+map.get("labeller")+"]";
//            }
//        }
        return ev;
    }
}
