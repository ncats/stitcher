package ncats.stitcher.dailymed;

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
import javax.xml.xpath.*;
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
public class DailyMedParser {
    static final Logger logger =
        Logger.getLogger(DailyMedParser.class.getName());

    static final QName AttrCodeSystem = new QName("codeSystem");
    static final QName AttrCode = new QName("code");
    static final QName AttrDisplayName = new QName("displayName");
    static final QName AttrClassCode = new QName("classCode");
    static final QName AttrValue = new QName("value");

    static final Pattern InitApprovalRe =
        Pattern.compile("approval[\\s]*[:\\-]?[\\D\\s\\n]*([0-9]{4})",
                        Pattern.CASE_INSENSITIVE);

    static final DateTimeFormatter DTF =
        DateTimeFormatter.ofPattern("yyyyMMdd");

    static public class Substance {
        public String name;
        public String unii;
        public List<Substance> activeMoieties = new ArrayList<>();
    }
    
    static public class Ingredient {
        public String code;
        public String amount;
        public Substance substance;
    }

    static public class Package {
        public String ndc;
        public String form;
    }
    
    static public class Product {
        public String name;
        public String genericName;
        public String formulation;
        public String route;
        public String approvalId;
        public String approvalAuthority;
        public String marketStatus;
        public String marketingStatus;
        public Calendar marketDate;
        public String ndc;
        public String equivNDC;
        public Boolean isPart;
        public List<Product> parts = new ArrayList<>();
        public List<Ingredient> ingredients = new ArrayList<>();
        public List<Package> packages = new ArrayList<>();
    }

    static public class Section {
        public String id;
        public String name;
        public String title;
        public String text;
    }

    static public class DrugLabel {
        public String id;      
        public Integer initialApprovalYear;
        public String productCategory;
        public String url;
        public String indications;
        public String comment;
        public List<Product> products = new ArrayList<>();
        public List<Section> sections = new ArrayList<>();
    }

    final ObjectWriter writer;
    File outdir;
    Set<String> tags = new TreeSet<>();

    public DailyMedParser() {
        ObjectMapper mapper = new ObjectMapper();   
        writer = mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
                        .writer(new SimpleDateFormat ("yyyy-MM-dd"))
                        .withDefaultPrettyPrinter()
                        .withoutFeatures(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS);
    }
    
    protected int parseXml0(InputStream is) throws Exception {
        XMLEventReader events = XMLInputFactory.newInstance().createXMLEventReader(is);
        
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
                } else if ("2.16.840.1.113883.4.9".equals(attr.getValue())) {
                    attr = se.getAttributeByName(AttrCode);
                    if (attr != null) {
                        StringBuilder path = new StringBuilder();

                        String ingredient = "";

                        for (Iterator<XMLEvent> it = stack.descendingIterator(); it.hasNext(); ) {
                            ev = it.next();

                            se = ev.asStartElement();

                            if ("ingredient".equals(se.getName().getLocalPart())) {
                                ingredient = se.getAttributeByName(AttrClassCode).getValue();
                            }

                            path.append("/" + se.getName().getLocalPart());
                        }
                        
                        System.out.println(attr.getValue() + "\t"
                                           + ingredient + "\t"
                                           + path);
                    }
                } else if ("2.16.840.1.113883.6.1".equals(attr.getValue())) {
                    // indication section
                    attr = se.getAttributeByName(AttrCode);
                    if (attr != null && "34067-9".equals(attr.getValue())) {
                        
                    }               
                }
            } else if (ev.isEndElement()) {
                stack.pop();
            }
        }
        events.close();
        return 0;
    }
    
    void getText(StringBuilder text, Node node) {
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
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
                getText(text, n);
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
    
    String getText(Node node) {
        StringBuilder text = new StringBuilder();
        getText(text, node);

        return text.toString();
    }

    Substance parseIngredientSubstance(Element el) {
        Substance sub = new Substance();
        NodeList children = el.getChildNodes();

        for (int i = 0; i < children.getLength(); i++) {
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
                    NodeList nl = child.getElementsByTagName("activeMoiety");
                    for (int j = 0; j < nl.getLength(); j++) {
                        child = (Element)nl.item(j);
                        sub.activeMoieties.add
                            (parseIngredientSubstance (child));
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
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            
            if (Node.ELEMENT_NODE == node.getNodeType()) {
                Element child = (Element) node;

                switch (child.getTagName()) {
                case "quantity":
                    NodeList nl = child.getElementsByTagName("numerator");

                    if (nl.getLength() > 0) {
                        Node n = nl.item(0);
                        double num = Double.parseDouble(n.getAttributes()
                                                         .getNamedItem("value")
                                                         .getTextContent());

                        String numu = n.getAttributes()
                                       .getNamedItem("unit")
                                       .getTextContent();
                        
                        nl = child.getElementsByTagName("denominator");

                        if (nl.getLength() > 0) {
                            n = nl.item(0);

                            double den = Double.parseDouble(n.getAttributes()
                                                             .getNamedItem("value")
                                                             .getTextContent());

                            String denu = n.getAttributes()
                                           .getNamedItem("unit")
                                           .getTextContent();

                            ingre.amount = String.format("%1$.1f", num/den) + " " + numu;

                            if (!denu.equals("1")){
                                ingre.amount += "/" + denu;
                            }
                        }
                    }
                    break;
                    
                case "ingredientSubstance":
                    ingre.substance = parseIngredientSubstance(child);
                    break;
                }
            }
        }
        
        return ingre;
    }

    void parsePackages(List<Package> packages, Node node) {
        NodeList children = node.getChildNodes();
        Package pkg = null;
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);

            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element child = (Element) n;

                switch (child.getTagName()) {
                case "code":
                    Node item = child.getAttributes()
                                     .getNamedItem("codeSystem");

                    if (item != null && "2.16.840.1.113883.6.69".equals
                        (item.getTextContent())) {
                        item = child.getAttributes().getNamedItem("code");
                        if (item != null) {
                            pkg = new Package ();
                            pkg.ndc = item.getTextContent();
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
                    parsePackages(packages, child);
                }
            }
        }
    }
    
    Product parseProduct(Element el) {
        Product product = new Product();

        NodeList children = el.getChildNodes();

        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (Node.ELEMENT_NODE != node.getNodeType()){
                continue;
            }
            
            Element child = (Element) node;

            switch (child.getTagName()) {
                case "code":
                    Node n = child.getAttributes().getNamedItem("codeSystem");
                    if (n != null && "2.16.840.1.113883.6.69".equals(n.getTextContent())) {
                        product.ndc = child.getAttributes()
                                           .getNamedItem("code")
                                           .getTextContent();
                    }
                    break;
                    
                case "name":
                    product.name = child.getTextContent();
                    break;
                    
                case "formCode":
                    product.formulation = child.getAttributes()
                                               .getNamedItem("displayName")
                                               .getTextContent();
                    break;
                    
                case "ingredient":
                    Ingredient ingre = parseIngredient(child);

                    if (ingre != null){
                        product.ingredients.add(ingre);
                    }
                    break;
                    
                case "asEntityWithGeneric":
                    NodeList nl = child.getElementsByTagName("name");

                    if (nl.getLength() > 0){
                        product.genericName = nl.item(0).getTextContent();
                    }
                    break;

                case "asEquivalentEntity":
                    nl = child.getElementsByTagName("code");

                    for (int j = 0; j < nl.getLength(); j++) {
                        n = nl.item(j);

                        Node cs = n.getAttributes().getNamedItem("codeSystem");

                        if (cs != null  
                            && "2.16.840.1.113883.6.69".equals(cs.getTextContent())) {
                            product.equivNDC = n.getAttributes()
                                                .getNamedItem("code")
                                                .getTextContent();
                        }
                    }
                    break;
                    
                case "asContent":
                    parsePackages(product.packages, child);
                    break;

                case "part":
                    // if there is a part product, parse it as product
                    nl = child.getElementsByTagName("partProduct");

                    for (int j = 0; j < nl.getLength(); j++) {
                        Product p = parseProduct((Element) nl.item(j));
                        p.isPart = true;
                        product.parts.add(p);
                    }
                    break;
            }
        }

        // add all approval information
        addApproval(el, product);

        return product;
    }

    /** Returns all direct children of node with specific tag name
     *  Note: getElementsByTagName(), finds ALL descendants! 
     *  Note2: we are not using this method at the moment.
     */
    static List<Node> getChildNodes(Element element, String name){
        ArrayList<Node> al = new ArrayList<Node>();
        NodeList children = element.getChildNodes();
        int chl = children.getLength();

        for(int i = 0; i < chl; i++){
            if(name.equals(children.item(i).getNodeName())){
               al.add(children.item(i));
            }
        }

        return al;
    }

    void addApproval(Element el, Product product) {
        // parent node *may* contain subjectOf tags
        // which in turn contain approval data
        Element elParent = (Element) el.getParentNode();
        NodeList prodNodes = elParent.getChildNodes();

        // go through every child node (tag) and remove those which are not "subjectOf"
        for (int n = 0; n < prodNodes.getLength(); n++) {
            Node pNode = prodNodes.item(n);

            if (pNode.getNodeType() == Node.ELEMENT_NODE
                && !((Element) pNode).getTagName()
                                     .equals("subjectOf")){

                elParent.removeChild(pNode);
            }
        }

        // get all elements of interest from the modified parent element
        prodNodes = elParent.getElementsByTagName("approval");
        Element approval = prodNodes.getLength() > 0 ? (Element) prodNodes.item(0) : null;
      
        prodNodes = elParent.getElementsByTagName("marketingAct");
        Element marketingAct = prodNodes.getLength() > 0 ? (Element) prodNodes.item(0) : null;

        prodNodes = elParent.getElementsByTagName("routeCode");
        Element route = prodNodes.getLength() > 0 ? (Element) prodNodes.item(0) : null;

        // parse each of those elements (if any), 
        // and add information of interest to the product object
        if (product.name != null && approval != null) {
            NodeList nl = approval.getElementsByTagName("id");

            // approval type
            if (nl.getLength() > 0) {
                Node n = nl.item(0).getAttributes()
                                   .getNamedItem("extension");

                if (n != null) {
                    product.approvalId = n.getTextContent()
                                          .replace("part", "21 CFR ");
                } else {
                    nl = approval.getChildNodes();

                    for (int j = 0; j < nl.getLength(); j++) {
                        n = nl.item(j);

                        if (n.getNodeType() == Node.ELEMENT_NODE
                            && ((Element) n).getTagName()
                                            .equals("code")) {
                            product.approvalId = n.getAttributes()
                                                  .getNamedItem("displayName")
                                                  .getTextContent();
                        }
                    }
                }
            }
            
            // jurisdiction
            nl = approval.getElementsByTagName("territory");

            if (nl.getLength() > 0) {
                nl = ((Element) nl.item(0))
                                  .getElementsByTagName("code");

                if (nl.getLength() > 0) {
                    product.approvalAuthority = nl.item(0)
                                                  .getAttributes()
                                                  .getNamedItem("code")
                                                  .getTextContent();
                }
            }
                     
            // Marketing Category 
            // (it is called "Marketing Status" on the website and in the output)
            nl = approval.getElementsByTagName("code");
            if (nl.getLength() > 0) {
                product.marketingStatus = nl.item(0)
                                            .getAttributes()
                                            .getNamedItem("displayName")
                                            .getTextContent();
            }

            if (route != null) {
                Node n = route.getAttributes()
                    .getNamedItem("displayName");
                if (n != null)
                    product.route = n.getTextContent();
            }
            
            if (marketingAct != null) {
                nl = marketingAct.getElementsByTagName("statusCode");
                if (nl.getLength() > 0) {
                    product.marketStatus = nl.item(0)
                                             .getAttributes()
                                             .getNamedItem("code")
                                             .getTextContent();
                }
                
                nl = marketingAct.getElementsByTagName("low");
                if (nl.getLength() > 0) {
                    String low = nl.item(0).getAttributes()
                        .getNamedItem("value").getTextContent();
                    try {
                        LocalDate date = LocalDate.parse(low, DTF);
                        product.marketDate = Calendar.getInstance();
                        product.marketDate.set(date.getYear(),
                                               date.getMonthValue()-1,
                                               date.getDayOfMonth());
                    } catch (Exception ex) {
                        logger.log(Level.SEVERE, 
                                   "Bogus date format: " + low, ex);
                    }
                }
            }
        }
    }

    void parseProducts(DrugLabel label, NodeList products) {
        /** 
         *  document structure:
         *  products -> common (attributes for all products)
         *  products -> product -> (optional product part - PARSED AS PRODUCT!) 
         *             -> ingredient -> ingredient attributes
            product -> product attributes

         *  <manufacturedProduct>
         *      <manufacturedProduct>
         *          <part> // optional
         *               <partProduct>
         *               ...
         *               </partProduct>
         *          </part>
         *      </manufacturedProduct>
         *      <subjectOf>
         *          <approval>
         *          ...
         *          </approval>
         *      </subjectOf>
         *  </manufacturedProduct>
         */

        for (int i = 0; i < products.getLength(); i++) {
            /** 
             *  each product in products is a "manufacturedProduct" tag
             *  and thus when iterating over products, 
             *  we need to check if there's another manufacturedProduct tag in it
             *  if there is -- it means it only contains data in "subjectOf" tags
             *  i.e. approval, marketingAct, route
             *  since these data are inside the higher-level manufacturedProduct tag
             *  they will be populated first -- and then can be consumed inside 
             */

            Element prod = (Element) products.item(i);

            // proceed only if there isn't another enclosed manufacturedProduct tag
            if (prod.getElementsByTagName("manufacturedProduct")
                    .getLength() == 0) {
                Product product = parseProduct(prod);
                
                if (!product.parts.isEmpty()){
                    // if there are product parts, 
                    // then add each product part to the label as a separate product
                    label.products.addAll(product.parts);
                } else {
                    label.products.add(product);
                }
            }
        }
    }

    void parseSections(DrugLabel label, NodeList sections) {
        for (int i = 0; i < sections.getLength(); i++) {
            Element section = (Element) sections.item(i);
            NodeList children = section.getChildNodes();

            Section sec = new Section ();
            for (int j = 0; j < children.getLength(); j++) {
                Node node = children.item(j);

                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element child = (Element) node;
                    
                    switch (child.getTagName()) {
                    case "id":
                        sec.id = child.getAttributes()
                                      .getNamedItem("root").getTextContent();
                        break; 
                        
                    case "code":
                        sec.name = child.getAttributes()
                                        .getNamedItem("displayName").getTextContent();
                        break;

                    case "title":
                        sec.title = getText(child);
                        break;
                        
                    case "text":
                        sec.text = getText(child);
                        break;
                        
                    case "excerpt":
                        { NodeList nl = child.getElementsByTagName("text");
                            StringBuilder text = new StringBuilder();

                            for (int k = 0; k < nl.getLength(); k++) {
                                text.append(getText(nl.item(k)) + "\n");
                            }

                            sec.text += text.toString();
                        }
                    break;
                    }
                
                }
            }
            
            if (sec.id != null) {
                if (sec.title != null 
                    && sec.text != null 
                    && sec.title.toUpperCase()
                                .startsWith("INDICATIONS")) {
                    label.indications = sec.text
                                           .replaceAll("\n","")
                                           .replaceAll("\t"," ")
                                           .trim();

                    if (label.indications != null){
                        if (label.indications.toUpperCase().contains("ALLERGENIC EXTRACT") 
                            || label.indications.toUpperCase().contains("POLLEN EXTRACT")) {
                            label.comment = "ALLERGENIC";
                        }
                    }
                }

                label.sections.add(sec);
            }
        }
    }

    void parsePreamble(DrugLabel label, NodeList nodes) {
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);

            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element child = (Element) node;

                switch (child.getTagName()) {
                case "title":
                    String text = getText(node);

                    Matcher matcher = InitApprovalRe.matcher(text);

                    if (matcher.find()) {
                        label.initialApprovalYear = Integer.parseInt(matcher.group(1));
                    }
                    break;
                    
                case "id":
                    label.id = child.getAttributes().getNamedItem("root").getTextContent();
                    break;
                    
                case "setId":
                    Node n = child.getAttributes().getNamedItem("root");

                    if (n != null) {
                        label.url = "https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid="
                                    + n.getTextContent();
                    }
                    break;
                    
                case "code":
                    n = child.getAttributes().getNamedItem("displayName");

                    if (n != null) {
                        label.productCategory = n.getTextContent().toUpperCase().trim();
                    }
                    break;
                }
            }
        }
    }

    void tags(Element node) {
        tags.add(node.getTagName());

        NodeList children = node.getChildNodes();

        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);

            if (n.getNodeType() == Node.ELEMENT_NODE) {
                tags((Element) n);
            }
        }
    }

    public Set<String> tags() { 
        return tags; 
    }
    
    protected int parseXml(InputStream is) throws Exception {
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

        Element doc = builder.parse(is).getDocumentElement();
        
        DrugLabel label = new DrugLabel();
        parsePreamble(label, doc.getChildNodes());
        parseProducts(label, doc.getElementsByTagName("manufacturedProduct"));
        parseSections(label, doc.getElementsByTagName("section"));

        Set<String> seen = new HashSet<>();
        for (Product p : label.products) {
            String marketDate = p.marketDate != null 
                                ? String.format("%1$04d-%2$02d-%3$02d", 
                                                p.marketDate.get(Calendar.YEAR),
                                                p.marketDate.get(Calendar.MONTH)+1, 
                                                p.marketDate.get(Calendar.DAY_OF_MONTH)) 
                                : "";

            for (Ingredient i : p.ingredients) {
                System.out.println(i.substance.unii + "\t"
                                   + (p.marketingStatus != null 
                                      ? p.marketingStatus : "") + "\t"
                                   + (label.productCategory != null 
                                      ? label.productCategory : "") + "\t"
                                   + marketDate + "\t"
                                   + (label.initialApprovalYear != null 
                                      ? label.initialApprovalYear : "") + "\t"
                                   + i.code + "\t"
                                   + (p.approvalId != null ? p.approvalId : "") + "\t"
                                   + (p.equivNDC != null ? p.equivNDC : "") + "\t"
                                   + (p.ndc != null ? p.ndc : "") + "\t"
                                   + (p.route != null ? p.route : "") + "\t"
                                   + "\""+i.substance.name.replaceAll("\n","").toUpperCase().trim()
                                   + "\""+"\t"+(p.genericName != null 
                                                ? ("\"" 
                                                   + p.genericName.replaceAll("\n", "")
                                                                  .toUpperCase().trim()
                                                   + "\"")
                                                : "") + "\t"
                                   + (label.url != null ? label.url : "") + "\t"
                                   + (label.indications != null ? label.indications : "") + "\t"
                                   + (label.comment != null ? label.comment : ""));
                //seen.add(i.substance.unii);
            }
        }

        if (outdir != null) {
            logger.info(label.id + (label.initialApprovalYear != null
                                    ? label.initialApprovalYear.toString() 
                                    : ""));
            
            File out = new File(outdir, label.id + ".json");
            
            try {
                FileOutputStream fos = new FileOutputStream(out);
                writer.writeValue(fos, label);
                fos.close();
            }
            catch (IOException ex) {
                logger.log(Level.SEVERE, "Can't write file: " + out, ex);
            }
        }
        
        return 0;
    }    

    InputStream getInputStream(ZipInputStream zis, ZipEntry ze) throws IOException {
        logger.info("$$ " + ze.getName() + " " + ze.getSize());

        if (ze.getSize() > 0l) {        
            byte[] buf = new byte[(int)ze.getSize()];
            for (int nb, tb = 0; (nb = zis.read(buf, tb, buf.length - tb)) > 0; tb += nb);
            
            return new ByteArrayInputStream(buf);
        }
        return null;
    }

    protected int parseZip(InputStream is) throws Exception {
        ZipInputStream zis = new ZipInputStream(is);

        for (ZipEntry ze; zis.available() > 0; ) {
            ze = zis.getNextEntry();

            if (ze != null && !ze.isDirectory()) {
                String name = ze.getName();
                
                if (name.endsWith(".zip")) {
                    is = getInputStream(zis, ze);

                    if (is != null){
                        parseZip(is);
                    }

                } else if (name.endsWith(".xml")) {
                    is = getInputStream(zis, ze);

                    if (is != null){
                        parseXml(is);
                    }
                }
            }
        }
        zis.close();
        
        return 0;
    }
    
    public int parse(InputStream is) throws Exception {
        BufferedInputStream bis = new BufferedInputStream(is);

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
            if ((magic[0] & 0xff) == 0x50 
                && (magic[1] & 0xff) == 0x4b 
                && (magic[2] & 0xff) == 0x03 
                && (magic[3] & 0xff) == 0x04) {
                // zip file
                nb = parseZip(bis);
            } else { // assume xml
                nb = parseXml(bis);
            }
        } else {
            logger.log(Level.SEVERE, "Premature EOF!");
            nb = -1;
        }
        
        return nb;
    }

    public void setOutDir(File outdir) {
        if (!outdir.exists()){
            outdir.mkdirs();
        }

        this.outdir = outdir;
    }

    public File getOutDir() { 
        return outdir; 
    }
    
    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: ncats.stitcher.dailymed.DailyMedParser"
                               + " [OutDir=OUTPUT] FILE...");
            System.exit(1);
        }

        System.out.println("UNII\t"+
                           "MarketingStatus\t"+
                           "ProductCategory\t"+
                           "MarketDate\t"+
                           "InitialYearApproval\t"+
                           "ActiveCode\t"+
                           "ApprovalAppId\t"+
                           "Equiv NDC\t"+
                           "NDC\t"+
                           "Route\t"+
                           "ActiveMoietyName\t"+
                           "GenericProductName\t"+
                           "URL\t"+
                           "Indications\t"+
                           "Comment");
        
        DailyMedParser daily = new DailyMedParser();

        for (int i = 0; i < argv.length; i++) {
            int pos = argv[i].indexOf('=');

            if (pos > 0) {
                if ("outdir".equalsIgnoreCase(argv[i].substring(0, pos))) {
                    File outdir = new File(argv[i].substring(pos+1));

                    daily.setOutDir(outdir);
                } else {
                    System.err.println("Unknown option: " + argv[i]);
                }
            } else {
                logger.info("***** parsing " + argv[i] + " ******");
                daily.parse(new FileInputStream(argv[i]));
            }
        }
    }
}
