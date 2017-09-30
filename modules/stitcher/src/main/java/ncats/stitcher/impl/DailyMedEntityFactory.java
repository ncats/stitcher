package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.net.URL;
import java.util.regex.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.text.SimpleDateFormat;

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
import com.fasterxml.jackson.databind.ObjectWriter;
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

    static class Package {
        public String ndc;
        public String form;
    }
    
    static class Product {
        public String name;
        public String genericName;
        public String formulation;
        public String route;
        public String approvalId;
        public String approvalAuthority;
        public String marketStatus;
        public Calendar marketDate;
        public String equivNDC;
        public List<Ingredient> ingredients = new ArrayList<>();
        public List<Package> packages = new ArrayList<>();
    }

    static class Section {
        public String id;
        public String name;
        public String text;
    }

    static class DrugLabel {
        public String id;      
        public Integer initialApprovalYear;
        public List<Product> products = new ArrayList<>();
        public List<Section> sections = new ArrayList<>();
    }

    ObjectWriter writer;
    File outdir;
    Set<String> tags = new TreeSet<>();

    public DailyMedEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public DailyMedEntityFactory (File dir) throws IOException {
        super (dir);
    }
    
    public DailyMedEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    protected void init () {
        super.init();
        ObjectMapper mapper = new ObjectMapper ();   
        writer = mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .writer(new SimpleDateFormat ("yyyy-MM-dd"))
            .withDefaultPrettyPrinter()
            .withoutFeatures(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)
            ;
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
            case Node.TEXT_NODE:
                if (n.getParentNode().getNodeType() == Node.ELEMENT_NODE) {
                    Element p = (Element)n.getParentNode();
                    String value = n.getNodeValue();
                    if (value != null && value.length() > 0) {
                        switch (p.getTagName()) {
                        case "content": case "item":
                            text.append(value);
                            break;
                            
                        case "title":
                            text.append(value.replaceAll("[\\s\\n]+",""));
                            break;
                            
                        case "paragraph": 
                            text.append(value.replaceAll("[\\s\\n]+"," "));
                            break;
                        }
                    }
                }
                break;

            case Node.ELEMENT_NODE:
                if ("linkHtml".equals(((Element)n).getTagName())) {
                    break;
                }
                // fall through
                    
            default:
                getText (text, n);
                if (n.getNodeType() == Node.ELEMENT_NODE) {
                    switch (((Element)n).getTagName()) {
                    case "paragraph": case "item": case "br":
                        text.append("\n");
                        break;
                    }
                }
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
                                    +" "+numu;
                                if (!denu.equals("1"))
                                    ingre.amount += "/"+denu;
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

    void parsePackages (List<Package> packages, Node node) {
        NodeList children = node.getChildNodes();
        Package pkg = null;
        for (int i = 0; i < children.getLength(); ++i) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element child = (Element)n;
                switch (child.getTagName()) {
                case "code":
                    { Node item = child.getAttributes()
                            .getNamedItem("codeSystem");
                        if (item != null && "2.16.840.1.113883.6.69".equals
                            (item.getTextContent())) {
                            item = child.getAttributes().getNamedItem("code");
                            if (item != null) {
                                pkg = new Package ();
                                pkg.ndc = item.getTextContent();
                            }
                        }
                    }
                    break;
                    
                case "formCode":
                    if (pkg != null && "2.16.840.1.113883.3.26.1.1".equals
                        (child.getAttributes().getNamedItem("codeSystem")
                         .getTextContent())) {
                        pkg.form = child.getAttributes()
                            .getNamedItem("displayName").getTextContent();
                        packages.add(pkg);
                    }
                    break;

                default:
                    parsePackages (packages, child);
                }
            }
        }
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

            case "asEquivalentEntity":
                { NodeList nl = child.getElementsByTagName("code");
                    for (int j = 0; j < nl.getLength(); ++j) {
                        Node n = nl.item(j);
                        Node cs = n.getAttributes().getNamedItem("codeSystem");
                        if (cs != null  && "2.16.840.1.113883.6.69"
                            .equals(cs.getTextContent())) {
                            product.equivNDC = n.getAttributes()
                                .getNamedItem("code").getTextContent();
                        }
                    }
                }
                break;
                
            case "asContent":
                parsePackages (product.packages, child);
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
                        Node n = nl.item(0).getAttributes()
                            .getNamedItem("extension");
                        if (n != null)
                            product.approvalId = n.getTextContent();
                        else {
                            nl = approval.getChildNodes();
                            for (int j = 0; j < nl.getLength(); ++j) {
                                n = nl.item(j);
                                if (n.getNodeType() == Node.ELEMENT_NODE
                                    && ((Element)n).getTagName()
                                    .equals("code")) {
                                    product.approvalId = n.getAttributes()
                                        .getNamedItem("displayName")
                                        .getTextContent();
                                }
                            }
                        }
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
                        LocalDate date = LocalDate.parse
                            (nl.item(0).getAttributes().getNamedItem("value")
                            .getTextContent(), DTF);
                        product.marketDate = Calendar.getInstance();
                        product.marketDate.set(date.getYear(),
                                               date.getMonthValue()-1,
                                               date.getDayOfMonth());
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

    void tags (Element node) {
        tags.add(node.getTagName());
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                tags ((Element)n);
            }
        }
    }

    public Set<String> tags () { return tags; }
    
    protected int registerXml (InputStream is) throws Exception {
        DocumentBuilder builder =
            DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Element doc = builder.parse(is).getDocumentElement();
        tags (doc);
        
        DrugLabel label = new DrugLabel ();
        parsePreamble (label, doc.getChildNodes());
        parseProducts (label, doc.getElementsByTagName("manufacturedProduct"));
        parseSections (label, doc.getElementsByTagName("section"));

        if (outdir != null) {
            logger.info(label.id+(label.initialApprovalYear != null
                                  ? label.initialApprovalYear.toString() : ""));
            File out = new File (outdir, label.id+".json");
            try {
                FileOutputStream fos = new FileOutputStream (out);
                writer.writeValue(fos, label);
                fos.close();
            }
            catch (IOException ex) {
                logger.log(Level.SEVERE, "Can't write file: "+out, ex);
            }
        }
        
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

    public void setOutDir (File outdir) {
        if (!outdir.exists())
            outdir.mkdirs();
        this.outdir = outdir;
    }

    public File getOutDir () { return outdir; }
    
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
                ("Usage: ncats.stitcher.impl.DailyMedEntityFactory DB [OutDir=OUTPUT] FILE...");
            System.exit(1);
        }
        
        DailyMedEntityFactory daily = new DailyMedEntityFactory (argv[0]);
        for (int i = 1; i < argv.length; ++i) {
            int pos = argv[i].indexOf('=');
            if (pos > 0) {
                if ("outdir".equalsIgnoreCase(argv[i].substring(0, pos))) {
                    File outdir = new File (argv[i].substring(pos+1));
                    daily.setOutDir(outdir);
                }
                else {
                    System.err.println("Unknown option: "+argv[i]);
                }
            }
            else {
                logger.info("***** registering "+argv[i]+" ******");
                //daily.register(new File (argv[i]));
                daily.register(new FileInputStream (argv[i]));
            }
        }
        System.out.println("TAGS: "+daily.tags());
        daily.shutdown();
    }
}
