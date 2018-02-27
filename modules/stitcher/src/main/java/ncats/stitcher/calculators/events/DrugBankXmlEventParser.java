package ncats.stitcher.calculators.events;

import ncats.stitcher.Util;
import ncats.stitcher.calculators.EventCalculator;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class DrugBankXmlEventParser extends EventParser {
    protected String id;
    public DrugBankXmlEventParser() {
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
                EventCalculator.logger.log(Level.SEVERE, "Can't parse xml document for "
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
                (new ByteArrayInputStream(xml.getBytes("utf8")))
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
            EventCalculator.logger.log(Level.SEVERE, "Bogus XML: "+xml, ex);
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
                        ev.date = EventCalculator.SDF.parse(n.getTextContent());
                    }
                    catch (Exception ex) {
                        EventCalculator.logger.warning("Bogus date format: "
                                       +n.getTextContent());
                    }
                    break;

                default:
                    map.put(n.getTagName(), n.getTextContent());
                }

                if (!approved) {
                    ev.kind = Event.EventKind.Marketed;
                }
                //only inclue comment if it's present
                String name = map.get("name");
                if(name !=null){
                    StringBuilder comment = new StringBuilder(name);

                    String labeller = map.get("labeller");
                    if(labeller !=null){
                        comment.append(" [").append(labeller).append("]");
                    }
                    ev.comment = comment.toString();
                }
//                    ev.comment = map.get("name")+" ["+map.get("labeller")+"]";
            }
        }
        return ev;
    }
}
