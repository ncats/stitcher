package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.net.URL;
import java.util.regex.*;
import java.time.*;
import java.time.format.DateTimeFormatter;

import javax.xml.stream.*;
import javax.xml.stream.events.*;
import javax.xml.namespace.QName;
import javax.xml.parsers.*;
import org.w3c.dom.*;

import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.security.DigestInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.annotation.JsonInclude;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/* extracting from dailymed xml
   Active/inactive
   UNII
   Marketing status 
   Marketing date
   Marketing authorization act
   Marketing authorization act date
   Dosage Form
   Route
   Dose
   Parent NDC (if possible)
   Indication section text
 */
public class DailyMedEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(DailyMedEntityFactory.class.getName());

    static final QName AttrCodeSystem = new QName ("codeSystem");
    static final QName AttrCode = new QName ("code");
    static final QName AttrDisplayName = new QName ("displayName");
    static final QName AttrClassCode = new QName ("classCode");
    static final QName AttrValue = new QName ("value");

    static final Pattern InitApprovalRe =
        Pattern.compile("approval[\\s]*[:\\-]?[\\D\\s\\n]*([0-9]{4})",
                        Pattern.CASE_INSENSITIVE);

    static final DateTimeFormatter DTF =
        DateTimeFormatter.ofPattern("yyyyMMdd");

    static class Substance {
        public String name;
        public String unii;
        public List<Substance> activeMoieties = new ArrayList<>();
    }
    
    static class Ingredient {
        public String code;
        public String amount;
        public Substance substance;
    }
    
    static class Product {
        public String name;
        public String genericName;
        public String formulation;
        public String route;
        public String approvalId;
        public String approvalAuthority;
        public String marketStatus;
        public LocalDate marketDate;
        public List<Ingredient> ingredients = new ArrayList<>();
    }

    static class Section {
        public String id;
        public String name;
        public String text;
    }

    static class DrugLabel {
        public String id;      
        public String title;
        public int initialApprovalYear;
        public List<Product> products = new ArrayList<>();
        public List<Section> sections = new ArrayList<>();
    }

    public DailyMedEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public DailyMedEntityFactory (File dir) throws IOException {
        super (dir);
    }
    
    public DailyMedEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    protected int registerXml0 (InputStream is) throws Exception {
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        String tag;
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                XMLEvent pev = stack.peek();
                stack.push(ev);
                
                StartElement se = ev.asStartElement();
                Attribute attr = se.getAttributeByName(AttrCodeSystem);
                if (attr == null) {
                }
                else if ("2.16.840.1.113883.4.9".equals(attr.getValue())) {
                    attr = se.getAttributeByName(AttrCode);
                    if (attr != null) {
                        StringBuilder path = new StringBuilder ();
                        String ingredient = "";
                        for (Iterator<XMLEvent> it = stack.descendingIterator();
                             it.hasNext(); ) {
                            ev = it.next();
                            se = ev.asStartElement();
                            if ("ingredient".equals
                                (se.getName().getLocalPart())) {
                                ingredient = se.getAttributeByName
                                    (AttrClassCode).getValue();
                            }
                            path.append("/"+se.getName().getLocalPart());
                        }
                        
                        System.out.println(attr.getValue()+"\t"
                                           +ingredient+"\t"+path);
                    }
                }
                else if ("2.16.840.1.113883.6.1".equals(attr.getValue())) {
                    // indication section
                    attr = se.getAttributeByName(AttrCode);
                    if (attr != null && "34067-9".equals(attr.getValue())) {
                        
                    }               
                }
            }
            else if (ev.isEndElement()) {
                stack.pop();
                //System.out.println("-"+tag);
            }
        }
        events.close();
        return 0;
    }
    
    void getText (StringBuilder text, Node node) {
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node n = children.item(i);
            switch (n.getNodeType()) {
                /*
            case Node.TEXT_NODE:
                { Node p = n.getParentNode();
                    if (p.getNodeType() == Node.ELEMENT_NODE
                        && (((Element)p).getTagName().equals("title")
                            || ((Element)p).getTagName().equals("content")))
                        text.append(n.getTextContent());
                }
                break;*/
                
            case Node.ELEMENT_NODE:
                { String tag = ((Element)n).getTagName();
                    if ("br".equals(tag)) {
                        text.append("\n");
                    }
                    else if ("title".equals(tag)
                             || "content".equals(tag)) {
                        text.append(n.getTextContent());
                    }
                    else if ("paragraph".equals(tag)) {
                        text.append(n.getTextContent()+"\n");
                    }
                    else if ("item".equals(tag)) {
                        text.append("+ "+n.getTextContent()+"\n");
                    }
                    else
                        getText (text, n);
                }
                break;
                
            default:
                getText (text, n);
            }
        }
    }
    
    String getText (Node node) {
        StringBuilder text = new StringBuilder ();
        getText (text, node);
        return text.toString();
    }

    Substance parseIngredientSubstance (Element el) {
        Substance sub = new Substance ();
        NodeList children = el.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node node = children.item(i);
            if (Node.ELEMENT_NODE == node.getNodeType()) {
                Element child = (Element)node;
                switch (child.getTagName()) {
                case "code":
                    sub.unii = child.getAttributes()
                        .getNamedItem("code").getTextContent();
                    break;
                    
                case "name":
                    sub.name = child.getTextContent();
                    break;
                    
                case "activeMoiety":
                    { NodeList nl = child.getElementsByTagName("activeMoiety");
                        for (int j = 0; j < nl.getLength(); ++j) {
                            child = (Element)nl.item(j);
                            sub.activeMoieties.add
                                (parseIngredientSubstance (child));
                        }
                    }
                    break;
                }
            }
        }
        return sub;
    }

    Ingredient parseIngredient (Element el) {
        Ingredient ingre = new Ingredient ();
        ingre.code = el.getAttributes()
            .getNamedItem("classCode").getTextContent();
        
        NodeList children = el.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node node = children.item(i);
            
            if (Node.ELEMENT_NODE == node.getNodeType()) {
                Element child = (Element)node;
                switch (child.getTagName()) {
                case "quantity":
                    { NodeList nl = child.getElementsByTagName("numerator");
                        if (nl.getLength() > 0) {
                            Node n = nl.item(0);
                            double num = Double.parseDouble
                                (n.getAttributes().getNamedItem("value")
                                 .getTextContent());
                            String numu = n.getAttributes()
                                .getNamedItem("unit").getTextContent();
                            
                            nl = child.getElementsByTagName("denominator");
                            if (nl.getLength() > 0) {
                                n = nl.item(0);
                                double den = Double.parseDouble
                                    (n.getAttributes().getNamedItem("value")
                                     .getTextContent());
                                String denu = n.getAttributes()
                                    .getNamedItem("unit").getTextContent();
                                ingre.amount = String.format("%1$.1f", num/den)
                                    +" "+numu+"/"+denu;
                            }
                        }
                    }
                    break;
                    
                case "ingredientSubstance":
                    ingre.substance = parseIngredientSubstance (child);
                    break;
                }
            }
        }
        
        return ingre;
    }
    
    Product parseProduct (Element el) {
        Product product = new Product ();
        NodeList children = el.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node node = children.item(i);
            if (Node.ELEMENT_NODE != node.getNodeType())
                continue;
            
            Element child = (Element)node;
            switch (child.getTagName()) {
            case "code":
                break;
                
            case "name":
                product.name = child.getTextContent();
                break;
                
            case "formCode":
                product.formulation = child.getAttributes()
                    .getNamedItem("displayName").getTextContent();
                break;
                
            case "ingredient":
                { Ingredient ingre = parseIngredient (child);
                    if (ingre != null)
                        product.ingredients.add(ingre);
                }
                break;
                
            case "asEntityWithGeneric":
                { NodeList nl = child.getElementsByTagName("name");
                    if (nl.getLength() > 0)
                        product.genericName = nl.item(0).getTextContent();
                }
                break;
            }
        }
        return product;
    }

    void parseProducts (DrugLabel label, NodeList products) {
        Element approval = null, marketingAct = null, route = null;
        for (int i = 0; i < products.getLength(); ++i) {
            Element prod = (Element)products.item(i);
            NodeList nodes = prod.getElementsByTagName("manufacturedProduct");
            if (nodes.getLength() == 0) {
                Product product = parseProduct (prod);
                if (product.name != null) {
                    NodeList nl = approval.getElementsByTagName("id");
                    if (nl.getLength() > 0) {
                        product.approvalId = nl.item(0).getAttributes()
                            .getNamedItem("extension").getTextContent();
                    }

                    nl = approval.getElementsByTagName("territory");
                    if (nl.getLength() > 0) {
                        nl = ((Element)nl.item(0))
                            .getElementsByTagName("code");
                        if (nl.getLength() > 0) 
                            product.approvalAuthority = nl.item(0)
                                .getAttributes().getNamedItem("code")
                                .getTextContent();
                    }
                    
                    if (route != null)
                        product.route = route.getAttributes()
                            .getNamedItem("displayName").getTextContent();
                    
                    nl = marketingAct.getElementsByTagName("statusCode");
                    if (nl.getLength() > 0) {
                        product.marketStatus = nl.item(0)
                            .getAttributes().getNamedItem("code")
                            .getTextContent();
                    }
                    
                    nl = marketingAct.getElementsByTagName("low");
                    if (nl.getLength() > 0) {
                        product.marketDate = LocalDate.parse
                            (nl.item(0).getAttributes().getNamedItem("value")
                             .getTextContent(), DTF);
                    }
                    
                    label.products.add(product);
                }
            }
            else {
                nodes = prod.getElementsByTagName("approval");
                approval = nodes.getLength() > 0 ? (Element)nodes.item(0)
                    : null;
                
                nodes = prod.getElementsByTagName("marketingAct");
                marketingAct = nodes.getLength() > 0 ? (Element)nodes.item(0)
                    : null;

                nodes = prod.getElementsByTagName("routeCode");
                route = nodes.getLength() > 0 ? (Element)nodes.item(0)
                    : null;
            }
        }
    }

    void parseSections (DrugLabel label, NodeList sections) {
        for (int i = 0; i < sections.getLength(); ++i) {
            Element section = (Element)sections.item(i);
            NodeList children = section.getChildNodes();

            Section sec = new Section ();
            for (int j = 0; j < children.getLength(); ++j) {
                Node node = children.item(j);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element child = (Element)node;
                    
                    switch (child.getTagName()) {
                    case "id":
                        sec.id = child.getAttributes()
                            .getNamedItem("root").getTextContent();
                        break;
                        
                    case "code":
                        sec.name = child.getAttributes()
                            .getNamedItem("displayName").getTextContent();
                        break;
                        
                    case "text":
                        sec.text = getText (child);
                        break;
                        
                    case "excerpt":
                        { NodeList nl = child.getElementsByTagName("text");
                            StringBuilder text = new StringBuilder ();
                            for (int k = 0; k < nl.getLength(); ++k)
                                text.append(getText (nl.item(k))+"\n");
                            sec.text += text.toString();
                        }
                    break;
                    }
                
                }
            }
            
            if (sec.id != null)
                label.sections.add(sec);
        }
    }

    void parsePreamble (DrugLabel label, NodeList nodes) {
        for (int i = 0; i < nodes.getLength(); ++i) {
            Node node = nodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element child = (Element)node;
                switch (child.getTagName()) {
                case "title":
                    { String text = getText (node);
                        Matcher matcher = InitApprovalRe.matcher(text);
                        if (matcher.find()) {
                            label.initialApprovalYear =
                                Integer.parseInt(matcher.group(1));
                        }
                        else {
                            //System.out.println("!!!"+text+"!!!");
                        }
                        label.title = text;
                    }
                    break;
                    
                case "id":
                    label.id = child.getAttributes()
                        .getNamedItem("root").getTextContent();
                    break;
                }
            }
        }
    }
    
    protected int registerXml (InputStream is) throws Exception {
        DocumentBuilder builder =
            DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Element doc = builder.parse(is).getDocumentElement();

        DrugLabel label = new DrugLabel ();
        parsePreamble (label, doc.getChildNodes());
        parseProducts (label, doc.getElementsByTagName("manufacturedProduct"));
        parseSections (label, doc.getElementsByTagName("section"));

        ObjectMapper mapper = new ObjectMapper ();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .writer().withDefaultPrettyPrinter()
            .withoutFeatures(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)
            .writeValue(System.out, label);
        
        return 0;
    }    

    InputStream getInputStream (ZipInputStream zis, ZipEntry ze)
        throws IOException {
        logger.info("$$ "+ze.getName()+" "+ze.getSize());
        
        byte[] buf = new byte[(int)ze.getSize()];
        for (int nb, tb = 0;
             (nb = zis.read(buf, tb, buf.length-tb)) > 0; tb += nb)
            ;
                
        return new ByteArrayInputStream (buf);
    }

    protected int registerZip (InputStream is) throws Exception {
        ZipInputStream zis = new ZipInputStream (is);
        for (ZipEntry ze; zis.available() > 0; ) {
            ze = zis.getNextEntry();
            if (ze != null && !ze.isDirectory()) {
                String name = ze.getName();
                
                if (name.endsWith(".zip")) {
                    registerZip (getInputStream (zis, ze));
                }
                else if (name.endsWith(".xml")) {
                    registerXml (getInputStream (zis, ze));
                }
                else { // ignore
                }
            }
        }
        zis.close();
        
        return 0;
    }
    
    public int register (InputStream is) throws Exception {
        BufferedInputStream bis = new BufferedInputStream (is);
        bis.mark(1024);

        byte[] magic = new byte[4];
        int nb = bis.read(magic, 0, magic.length);
        if (nb == magic.length) {
            /*
            System.out.println(String.format("%1$02x", magic[0]&0xff)+" "+
                               String.format("%1$02x", magic[1]&0xff)+" "+
                               String.format("%1$02x", magic[2]&0xff)+" "+
                               String.format("%1$02x", magic[3]&0xff));
            */
            bis.reset(); // reset the buffer
            // check for zip magic 0x04034b50 as little endian
            if ((magic[0] & 0xff) == 0x50 &&
                (magic[1] & 0xff) == 0x4b &&
                (magic[2] & 0xff) == 0x03 &&
                (magic[3] & 0xff) == 0x04) {
                // zip file
                nb = registerZip (bis);
            }
            else { // assume xml
                nb = registerXml (bis);
            }
        }
        else {
            logger.log(Level.SEVERE, "Premature EOF!");
            nb = -1;
        }
        
        return nb;
    }

    @Override
    public DataSource register (File file) throws IOException {
        this.source = getDataSourceFactory().register(file);
        
        Integer instances = (Integer) this.source.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+this.source.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            try {
                instances = register (this.source.openStream());
                updateMeta (this.source);
                this.source.set(INSTANCES, instances);
                logger.info
                    ("$$$ "+instances+" entities registered for "+this.source);
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't register data source: "+file,
                           ex);
            }
        }
        
        return this.source;     
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: ncats.stitcher.impl.DailyMedEntityFactory DB FILE...");
            System.exit(1);
        }
        
        DailyMedEntityFactory daily = new DailyMedEntityFactory (argv[0]);
        for (int i = 1; i < argv.length; ++i) {
            logger.info("***** registering "+argv[i]+" ******");
            //daily.register(new File (argv[i]));
            daily.register(new FileInputStream (argv[i]));
        }
        daily.shutdown();
    }
}
