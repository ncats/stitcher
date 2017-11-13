package ncats.stitcher.tools;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;
import ncats.stitcher.*;
import ncats.stitcher.Entity;
import ncats.stitcher.Component;
import ncats.stitcher.graph.UnionFind;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.function.Function;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import static ncats.stitcher.StitchKey.*;
import static ncats.stitcher.StitchKey.H_LyChI_L3;
import static ncats.stitcher.StitchKey.N_Name;


/**
 * Created by williamsmard on 2/22/17.
 */
public class ReportGenerator {
    private static final String FDA = "Drugs@FDA";
    private static final String RANCHO = "Rancho 2017";
    private static final String GSRS = "fullSeedData-2016-06-16.gsrs";
    private static final String NAME = "N_Name";
    private static final String PHASE = "HighestPhase";
    private static final String EARLIEST = "EarliestDate";
    private static final String UNII = "I_UNII";
    private static final String LYCHII = "H_LyChI_L3";
    private static final String ACTIVE_MOIETY ="T_ActiveMoiety";

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm");
    private final EntityFactory ef;
    private final DataSourceFactory dsf;


    public static void main (String[] argv) throws IOException {


        if(argv.length<3)
        {
            System.err.println("Usage: "+ReportGenerator.class.getName()
                    +" DB OUTFILE REPORT [...PROPERTIES...]");
            System.exit(1);
        }
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        File file = new File(argv[1]+sdf.format(timestamp));
        Set<String> props = new HashSet<>();
        Reports report = getEnum(argv[2]);

        for(int i = 3; i<argv.length;i++)
        {
            props.add(argv[i]);
        }
        try
        {
            ReportGenerator rg = new ReportGenerator(graphDb);
            switch(report)
            {
                case APPROVED:
                    rg.generateApprovedReport(file,props);
                    break;
                case MULTILYCHII:
                    rg.generateMultiLychiiReport(file,props);
                    break;
                case EARLIEST:
                    rg.generateEarliestApprovalReport(file, props);
                    break;
                case DRUGPAGE:
                    rg.generateDrugPageReport(file, props, graphDb);
            }
        }
        finally
        {
            graphDb.shutdown();
        }

    }
    private ReportGenerator(GraphDb graphDb) {
        ef = new EntityFactory(graphDb);
        dsf = new DataSourceFactory(graphDb);
    }

    private void generateDrugPageReport(File output, Set<String>props, GraphDb graphDb)
    {

        //String out ="NAME\tHIGHEST PHASE\tEARLIEST APPROVAL\tACTIVE MOEITY UNIIS\tOTHER UNIIS\tLYCHIIS\n";
        String problem ="";

        Collection<ncats.stitcher.Component> components = ef.components();
        for(Component c : components) {
            String out = "";
            Entity[] entityArray = c.entities();
            Set<Entity> entities = new HashSet<>();
            Collections.addAll(entities,entityArray);
            Set<String> uniis, lychiis, activeUniis;
            String earliest = "NONE";
            String highestPhase = "NOTPROVIDED";
            Optional<LocalDate> earliestDate = findEarliestApprovalDate(entities);
            if (earliestDate.isPresent()) {
                earliest = earliestDate.get().toString();
            }
            HashMap payloads = new HashMap<>();
            for (Entity e : entities) {
                if (isApproved(e)) {
                    highestPhase = "APPROVED";
                }

                for (Map.Entry<String, Object> me: e.payload().entrySet()) {
                    Object obj = payloads.get(me.getKey());
                    if (obj == null) {
                        payloads.put(me.getKey(), obj = new ArrayList());
                    }

                    if (me.getValue().getClass().isArray()) {
                        for (int i = 0; i < Array.getLength(me.getValue()); ++i)
                            ((List)obj).add(Array.get(me.getValue(), i));
                    }
                    else {
                        ((List)obj).add(me.getValue());
                    }
                }
            }
            if (!highestPhase.equals("APPROVED")) {
                highestPhase = getHighestPhase(entities).name();
            }

            activeUniis=uniqueSet(findBestUnii(entities));
            lychiis= uniqueSet(getStringProp(entities,LYCHII));
            uniis = uniqueSet(getStringProp(entities,UNII));
            uniis.removeAll(activeUniis);
            List<String> names = new ArrayList<String>(getStringProp(entities,NAME));
            Comparator<String> byLength = (e1, e2) -> e1.length() > e2.length() ? -1 : 1;
            String name = names.stream().sorted(byLength.reversed()).findFirst().get();
            List<Molecule> mols = getMols(entities);
            Molecule smallest = new Molecule();
            if(!mols.isEmpty())
            {
                try{
                    smallest = mols.stream().min((o1,o2)->o1.getAtomCount()-o2.getAtomCount()).get();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            smallest.setProperty("name",name);
            smallest.setProperty("highestPhase",highestPhase);
            smallest.setProperty("earliestApproval",earliest);
            smallest.setProperty("activeUniis",newLineDelimitedOf(activeUniis));
            smallest.setProperty("otherUniis",newLineDelimitedOf(uniis));
            smallest.setProperty("lychiis",newLineDelimitedOf(lychiis));
            for (Object me : payloads.entrySet()) {
                StringBuilder sb = new StringBuilder ();
                List value = (List)((Map.Entry)me).getValue();
                if(!value.isEmpty())
                {
                    sb.append(value.get(0).toString());
                }
                for (int i = 1; i < value.size(); ++i)
                    sb.append("\n"+value.get(i).toString());
                smallest.setProperty("_"+((Map.Entry)me).getKey(), sb.toString());
            }
            smallest.setProperty("ClusterSize",String.valueOf(c.size()));
            List<RedFlag> flags = generateRedFlag(c);
            if(!flags.isEmpty())
            {
                smallest.setProperty("RedFlag",String.valueOf(c.getId()));
                for(RedFlag flag : flags)
                {
                    smallest.setProperty(flag.getType(),flag.getDetails());
                }
            }
            out=smallest.toFormat("sdf");
            printLog(out,output);
        }
    }
    public class RedFlag{
        private String type;
        private String details;
        private String id;
        public RedFlag(String id, String type, String details)
        {
            this.id = id;
            this.type = type;
            this.details=details;
        }
        public RedFlag()
        {

        }
        public String getType(){
            return type;
        }
        public String getDetails() {
            return details;
        }

        public String getId() {
            return id;
        }
        public void setId(String id)
        {
            this.id=id;
        }
        public void setDetails(String details)
        {
            this.details=details;
        }
        public void setType(String type)
        {
            this.type=type;
        }
        public String toString()
        {
            return id+","+type+","+details;
        }

    }
    public void generateRedFlagReport(File outFile)
    {
        Collection<ncats.stitcher.Component> components = ef.components();
        StringBuilder sb = new StringBuilder();
        for(Component c : components)
        {
            List<RedFlag> flags = generateRedFlag(c);
            if(!flags.isEmpty()){
                sb.append(getName(c)).append("\n");
                flags.forEach(flag->{
                    sb.append(flag.toString()).append("\n");
                });
                sb.append("\n");
            }
        }
        printLog(sb.toString(),outFile);
    }
    public List<RedFlag> generateRedFlag(Component c)
    {
        List<RedFlag> redFlags = new ArrayList<RedFlag>();
        String id = c.getId();
        //hack for performance reasons; running untangle on UREA is a huge time suck
        if(getName(c).equals("UREA"))
        {
            redFlags.add(new RedFlag(c.getId(),"UREA","UREA"));
            return redFlags;
        }
        List<Component> untangled = untangle(c);
        if(untangled.size()>1){
            RedFlag tangled = new RedFlag();
            tangled.setType("TANGLED");
            StringBuilder sb = new StringBuilder();
            for(Component ut: untangled)
            {
                sb.append(getName(ut)).append("|");
            }
            tangled.setDetails(sb.deleteCharAt(sb.length() - 1).toString());
            tangled.setId(id);
            redFlags.add(tangled);
        }
        Set<Entity> entities = new HashSet<>(Arrays.asList(c.entities()));
        List<Entity> ruili = ruiliEntities(entities);
        if(ruili.size()>1){
            RedFlag ruiliFlag = new RedFlag();
            ruiliFlag.setType("MultipleRuili");
            StringBuilder sb = new StringBuilder();
            for(Entity r : ruili)
            {
                sb.append(r.getStringProp(NAME).get()).append("|");
            }
            ruiliFlag.setDetails(sb.deleteCharAt(sb.length() - 1).toString());
            ruiliFlag.setId(id);
            redFlags.add(ruiliFlag);
        }
        return redFlags;
    }
    public String getName(Component c)
    {
        Set<Entity> entities = new HashSet<>();
        entities.addAll(Arrays.asList(c.entities()));
        List<String> names = new ArrayList<String>(getStringProp(entities,NAME));
        Comparator<String> byLength = (e1, e2) -> e1.length() > e2.length() ? -1 : 1;
        String name = names.stream().sorted(byLength.reversed()).findFirst().get();
        return name;
    }
    public List<Component> untangle (Component component) {

        //hacky shenanigans to swallow all the STDOUT from this method
        //TODO refactor this method for our usecase instead of butchering Trung's code
        PrintStream original = System.out;
        System.setOut(new PrintStream(new OutputStream() {
            public void write(int b) {
                //DO NOTHING
            }
        }));


        Util.dump(component);
        UnionFind uf = new UnionFind ();

        Set<Long> promiscous = new TreeSet<>();
        // first only consider priority 5 keys
        for (StitchKey k : StitchKey.keys(5, 5)) {
            Map<Object, Integer> stats = component.stats(k);
            for (Map.Entry<Object, Integer> v : stats.entrySet()) {
                long[] nodes = component.nodes(k, v.getKey());

                if (nodes.length > 0) {
                    int union = 0;
                    for (int i = 0; i < nodes.length; ++i) {
                        Entity e = ef.entity(nodes[i]);
                        Object val = e.keys().get(k);
                        assert (val != null);
                        System.out.print(i > 0 ? "," : "[");
                        System.out.print(nodes[i]);
                        if (val.getClass().isArray()
                                && Array.getLength(val) > 1) {
                            // this node has more than one stitches, so let's
                            // deal with it later
                            promiscous.add(nodes[i]);
                        }
                        else {
                            uf.add(nodes[i]);
                            for (int j = 0; j < i; ++j)
                                if (!promiscous.contains(nodes[j]))
                                    uf.union(nodes[i], nodes[j]);
                        }
                    }
                    System.out.println("]");
                }
            }
        }

        long[][] clumps = uf.components();

        System.out.println("************** 1. Refined Clusters ****************");
        for (int i = 0; i < clumps.length; ++i) {
            System.out.print((i+1)+" "+clumps[i].length+": ");
            for (int j = 0; j < clumps[i].length; ++j) {
                System.out.print((j==0 ? "[":",")+clumps[i][j]);
                Map<StitchValue, long[]> extent = ef.expand(clumps[i][j]);
                for (Map.Entry<StitchValue, long[]> me : extent.entrySet()) {
                    StitchKey key = me.getKey().getKey();
                    if (key.priority > 0) {
                        long[] nodes = me.getValue();
                        for (int k = 0; k < nodes.length; ++k)
                            if (!uf.contains(nodes[k]))
                                uf.union(clumps[i][j], nodes[k]);
                    }
                }
            }
            System.out.println("]");
        }

        // now assign promiscous nodes
        System.out.println("************** 2. Refined Clusters ****************");
        clumps = uf.components();
        long[] nodes = Util.toArray(promiscous);

        Map<Long, Set<String>> colors = new TreeMap<>();
        List<Component> components = new ArrayList<>();
        for (int i = 0; i < clumps.length; ++i) {
            System.out.print((i+1)+" "+clumps[i].length+": ");
            for (int j = 0; j < clumps[i].length; ++j)
                System.out.print((j==0 ? "[" : ",")+clumps[i][j]);
            System.out.println("]");

            Component comp = ef.component(clumps[i]);
            comp = comp.add(nodes, StitchKey.keys(5, 5));

            long[] nc = comp.nodes();
            for (int j = 0; j < nc.length; ++j) {
                Set<String> c = colors.get(nc[j]);
                if (c == null)
                    colors.put(nc[j], c = new HashSet<>());
                c.add(comp.getId());
            }

            components.add(comp);
        }

        Set<Long> leftover = new TreeSet<>(component.nodeSet());
        for (Component c : components) {
            promiscous.removeAll(c.nodeSet());
            leftover.removeAll(c.nodeSet());
        }

        System.out.println("************** Promicuous ****************");
        System.out.println(promiscous);

        System.out.println("************** Leftover Nodes ******************");
        System.out.println(leftover);
        for (Long n : leftover) {
            System.out.print("+"+n+":");
            component.depthFirst(n, p -> {
                System.out.print("<");
                for (int i = 0; i < p.length; ++i)
                    System.out.print((i==0?"":",")+p[i]);
                System.out.print(">");
            }, H_LyChI_L4, N_Name);
            System.out.println();
        }
        if (!leftover.isEmpty()) {
            Component c = ef.component(Util.toArray(leftover));
            Util.dump(c);

            Map<Long, Set<Long>> cands = new TreeMap<>();
            for (StitchKey k : new StitchKey[]{
                    // in order of high to low priority
                    H_LyChI_L4, I_UNII, I_CAS,
                    N_Name, H_LyChI_L3
            }) {
                for (Object v : c.values(k).keySet()) {
                    System.out.println(">>> cliques for "+k+"="+v+" <<<");
                    c.cliques(clique -> {
                        long[] nc = clique.nodes();
                        Set<Long> unmapped = new TreeSet<>();
                        Set<Long> mapped = new TreeSet<>();
                        for (int i = 0; i < nc.length; ++i) {
                            System.out.print(" "+nc[i]);
                            Set<String> s = colors.get(nc[i]);
                            if (s != null) {
                                Iterator<String> it = s.iterator();
                                System.out.print("["+it.next());
                                while (it.hasNext()) {
                                    System.out.print(","+it.next());
                                }
                                System.out.print("]");
                                mapped.add(nc[i]);
                            }
                            else {
                                unmapped.add(nc[i]);
                            }
                        }
                        System.out.println();

                        if (mapped.isEmpty()) {
                            // create a new component
                            Component comp = ef.component(nc);
                            Set<String> s =
                                    Collections.singleton(comp.getId());
                            for (int i = 0; i < nc.length; ++i)
                                colors.put(nc[i], s);
                        }
                        else {
                            // now assign each unmapped node to the best
                            // component
                            for (Long n : unmapped) {
                                Set<Long> m = cands.get(n);
                                if (m != null)
                                    m.addAll(mapped);
                                else
                                    cands.put(n, mapped);
                            }
                        }

                        return true;
                    }, k, v);
                }
            }

            System.out.println("************** Candidate Mapping ****************");
            for (Map.Entry<Long, Set<Long>> me : cands.entrySet()) {
                System.out.print(me.getKey()+":");
                for (Long n : me.getValue()) {
                    System.out.print(" "+n);
                    Set<String> s = colors.get(n);
                    if (s != null) {
                        Iterator<String> it = s.iterator();
                        System.out.print("["+it.next());
                        while (it.hasNext()) {
                            System.out.print(","+it.next());
                        }
                        System.out.print("]");
                    }
                }
                System.out.println();
            }
        }

        System.out.println("************** Final Components ****************");
        for (Component c : components) {
            Util.dump(c);
        }

        System.out.println("####### "+components.size()+" components! #######");
        System.setOut(original);
        return components;
    }

    private List<Entity> ruiliEntities(Set<Entity> entities)
    {
        List<Entity> ruili = new ArrayList<>();
        for(Entity e: entities)
        {
            if(e.datasource().getName().contains("Ruili's"))
            {
                ruili.add(e);
            }
        }
        return ruili;
    }
    private String bestName(Set<Entity> entities)
    {
        for(Entity e : entities)
        {
            Optional<String> name = e.getStringProp(NAME);
            Optional<String> moiety = e.getStringProp(ACTIVE_MOIETY);
            Optional<String> unii = e.getStringProp(UNII);

            if(name.isPresent() &&
                    moiety.isPresent() &&
                    unii.isPresent()){
                if(unii.get().equals(moiety.get()))
                {
                    return name.get();
                }
            }
        }
        return null;
    }
    private List<Molecule> getMols (Set<Entity> entities)
    {
        List<Molecule> mols = new ArrayList<>();
        for(Entity e: entities)
        {
            Set<String> molSet = e.getPayloadValues("MOLFILE");
            if(molSet.size()>1)
            {
                System.err.println("Multiple mols?");
            }
            else if(!molSet.isEmpty())
            {
                try
                {
                    MolHandler mh = new MolHandler(molSet.iterator().next());
                    Molecule mol = mh.getMolecule();
                    mols.add(mol);
                }
                catch(Exception ex)
                {
                    System.err.println("bad parse");
                }

            }
        }
        return mols;
    }
    private Set<String> getMoiety(Set<Entity> entities)
    {
        Set<String> trueMoieties = new HashSet<>();
        Set<Entity> otherMoieties = new HashSet<>();
        Entity moiety = null;
        GraphDatabaseService db = entities.iterator().next().getGraphDb();
        try(Transaction tx = db.beginTx())
        {
            for(Entity e : entities)
            {
                Optional<Entity> am = e._getActiveMoiety();
                if(am.isPresent())
                {
                    if(am.get()._getActiveMoiety().equals(am)){
                        trueMoieties.add(am.get().getStringProp(ACTIVE_MOIETY).get());

                    }
                    else
                    {
                        otherMoieties.add(am.get());
                    }
                }
            }
        }
        List<String> names = new ArrayList<String>(getStringProp(entities,"N_Name"));
        Comparator<String> byLength = (e1, e2) -> e1.length() > e2.length() ? -1 : 1;
        String name = names.stream().sorted(byLength.reversed()).findFirst().get();
//        if(!otherMoieties.isEmpty() || !trueMoieties.isEmpty())
//        {
//            if(trueMoieties.size()>1)
//            {
//                System.out.println("HAS MULTIPLE TRUE MOIETIES");
//                System.out.println(entities.iterator().next().getId()+" "+name+" "+trueMoieties.size()+" "+otherMoieties.size());
//            }
//            if(!otherMoieties.isEmpty())
//            {
//                System.out.println("HAS FALSE MOIETIES");
//                System.out.println(entities.iterator().next().getId()+" "+name+" "+trueMoieties.size()+" "+otherMoieties.size());
//            }
//        }

        return trueMoieties;
    }
    private String commaDelimitedOf(Set<String> set)
    {
        if(set.isEmpty())
        {
            return "NONE";
        }
        StringBuilder sb = new StringBuilder();
        for(String s : set)
        {
            sb.append(s).append(",");
        }
        return sb.deleteCharAt(sb.length() - 1).toString();

    }
    private String newLineDelimitedOf(Set<String> set)
    {
        if(set.isEmpty())
        {
            return "NONE";
        }
        StringBuilder sb = new StringBuilder();
        for(String s : set)
        {
            sb.append(s).append("\n");
        }
        return sb.deleteCharAt(sb.length() - 1).toString();

    }
    private Set<String> uniqueSet(Set<String> set)
    {
        Set<String> unique = new HashSet<>();
        for(String unii : set)
        {
            if(unii.contains(","))
            {
                List<String> list = Arrays.asList(unii.split(","));
                for(String item : list)
                {
                    unique.add(item);
                }
            }
            else
            {
                unique.add(unii);
            }
        }
        return unique;
    }
    private void generateApprovedReport(File output, Set<String>props)
    {
        Set<Entity> approved = this.findApprovedDrugs();
        printLog(approved,props,output);
    }
    private void generateMultiLychiiReport(File output, Set<String>props)
    {
        Set<Entity>approved = this.findApprovedDrugs();
        Set<Entity>multiLychii = new HashSet<Entity>();

        for(Entity e : approved)
        {
            if(getStringPropBroad(e,"H_LyChI_L3").size()>1)
            {
                multiLychii.add(e);
            }
        }
        printLog(multiLychii,props,output);
    }
    /*
        Currently throwing away the props set and just printing name and earliest approval date
     */
   private void generateEarliestApprovalReport(File output, Set<String>props)
    {
        StringBuffer report = new StringBuffer();
        Set<Entity> approved = this.findApprovedDrugs();
        for(Entity entity : approved )
        {
            Optional<LocalDate>date = findEarliestApprovalDate(entity);
            if(date.isPresent())
            {
                report.append(entity.getStringProp("N_Name").get()+"\t"+date.get()+"\n");
            }
        }
        printLog(report.toString(),output);
    }


    private Set<Entity> findApprovedDrugs()
    {
        Set<Entity> nodes = new HashSet<Entity>();
        Iterator<Entity> components= ef.entities("COMPONENT");

        int count = 0;
        while(components.hasNext()) {
            Entity comp = components.next();

            count++;
            Entity[] neighbors = comp.neighbors();
            if (isApproved(comp)) {
                nodes.add(comp);
                continue;
            }

            for (Entity neighbor : neighbors) {
                if (isApproved(neighbor)) {
                    nodes.add(comp);

                }
            }
        }
        return nodes;
    }

    private static boolean isApproved(Entity e)
    {
        if(e.datasource().getName().equals(FDA))
        {
            return true;
        }
        else if(e.datasource().getName().equals(RANCHO))
        {

            if(e.payload().keySet().contains("HighestPhase"))
            {
                //Optional<String> highest=getStringProp(e,"HighestPhase");
                String highest="";
                if(e.payload().get("HighestPhase")instanceof String)
                {
                    highest = (String)e.payload().get("HighestPhase");
                }
                else
                {
                    String[] highArray= (String[])e.payload().get("HighestPhase");
                    if(highArray[0] != null)
                    {
                        highest = highArray[0];
                    }
                }

                if(highest.equals("Approved"))
                {
                    return true;
                }

            }
        }
        return false;
    }
    private Optional<LocalDate> findEarliestApprovalDate(Set<Entity> entities)
    {
        final String EARLIEST = "EarliestDate";
        Iterator<Entity> iter = entities.iterator();
        List<LocalDate> dates = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDate earliest = null;
        while(iter.hasNext())
        {
            Entity e = iter.next();
            Optional<LocalDate> date = findEarliestApprovalDate(e);
            if(date.isPresent())
            {
                dates.add(date.get());
            }
        }
        if(!dates.isEmpty())
        {
            Collections.sort(dates);
            earliest=dates.get(0);
            dates.clear();
        }
        return Optional.ofNullable(earliest);
    }
    private Optional<LocalDate> findEarliestApprovalDate(Entity e)
    {
        LocalDate earliest = null;
        List<LocalDate> dates = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        Set<String> dateSet = e.getPayloadValues(EARLIEST);
        for(String date : dateSet)
        {
            if(date.equals("Not given"))
            {
                continue;
            }
            try{
                dates.add(formatter.parseLocalDate(date));
            }
            catch(Exception exception)
            {
                exception.printStackTrace();
            }
        }
        if(!dates.isEmpty())
        {
            Collections.sort(dates);
            earliest = dates.get(0);
        }
        return Optional.ofNullable(earliest);
    }
    private static String findBestUnii(Entity e)
    {
        return null;
    }
    private Phases getHighestPhase(Set<Entity> entities)
    {
        Phases highest = Phases.valueOf("NOTPROVIDED");
        for(Entity e : entities)
        {
            Set<Phases> entityPhases = getHighestPhases(e);
            for (Phases current: entityPhases)
                if(current.ordinal()<highest.ordinal())
                {
                    highest=current;
                }
        }
        return highest;
    }
    private Set<Phases> getHighestPhases(Entity e)
    {
        String noPhase = "NOTPROVIDED";
        Set<Phases> highs = new HashSet<Phases>();
        for (String phaseValue: e.getPayloadValues("HighestPhase")) {
            phaseValue=phaseValue.toUpperCase().replaceAll("\\s","");
            for (String phase: phaseValue.split("\\|")) {
                if(phase.equals("NATURALMETABOLITE")) {
                    phase="CLINICAL";
                }
                try {
                    highs.add(Phases.valueOf(phase));
                } catch (Exception ex) {
                    highs.add(Phases.valueOf(noPhase));
                }
            }
        }
        if(highs.isEmpty())
        {

            //TODO find the real names for USAN and JAN used in the db
            Set<String> inn = e.getPayloadValues("INN");
            Set<String> usan = e.getPayloadValues("USAN");
            Set<String> jan = e.getPayloadValues("JAN");
            if(!inn.isEmpty()||!usan.isEmpty()||!jan.isEmpty())
            {
                highs.add(Phases.valueOf("CLINICAL"));
            }

        }
        return highs;
    }
    private Set<String> findBestUnii(Set<Entity> cluster)
    {
        Set<String> best= new HashSet<>();
        for(Entity e: cluster)
        {
            if (e.getStringProp("T_ActiveMoiety").isPresent()) {
                  best.add(e.getStringProp("T_ActiveMoiety").get());
            }
        }
        return best;
    }
    private static void printLog(String s, File file)
    {
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(s);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void printLog(Set<Entity> e, Set<String> props, File file)
    {
        String legend ="";
        int count = 0;
        Iterator<String> iter = props.iterator();
        while(iter.hasNext())
        {
            legend = legend + iter.next()+"\t";
        }
        legend = legend+"\n";
        Iterator<Entity> ei =e.iterator();
        String output = legend;
        boolean first;
        while(ei.hasNext())
        {
            count++;
            String line = "";
            Entity entity = ei.next();
            Iterator<String> pi = props.iterator();
            first = true;
            while(pi.hasNext())
            {
                String prop = pi.next();

                //We don't generally want multiple values for the first property in the list
                if(first)
                {
                    if(entity.getStringProp(prop).isPresent())
                    {
                        line = line + entity.getStringProp(prop).get()+"\t";
                        continue;
                    }
                }
                Set<String> propVals = getStringPropBroad(entity,prop);
                if(propVals.size()>1)
                {
                    StringBuilder sb = new StringBuilder();
                    for(String p : propVals)
                    {
                        sb.append(p).append(",");
                    }
                    line = line+sb.deleteCharAt(sb.length() - 1).toString()+"\t";
                }
                else
                {
                    Iterator<String> si = propVals.iterator();
                    if(si.hasNext()) {
                        line = line + si.next() + "\t";
                    }
                    else
                    {
                        line = line +"NONE\t";
                    }
                }
                first = false;
            }
            output = output+line+"\n";
        }
        printLog(output,file);

    }
    public static Set<String> getStringPropBroad(Entity e, String s)
    {
        Set<Entity> hasProp = new HashSet<Entity>();
        Set<String> propValues = new HashSet<String>();
        if(e.properties().containsKey(s))
        {
            e.getStringProp(s).ifPresent(prop->{
                propValues.add(prop);
            });
        }

        Entity[] neighbors = e.neighbors();
        for(Entity neighbor:neighbors)
        {
            if(neighbor.properties().containsKey(s))
            {
               if(neighbor.getStringProp(s).isPresent())
               {
                   propValues.add(neighbor.getStringProp(s).get());
               }

            }
        }
        return propValues;
    }
    public static Set<String> getStringProp(Set<Entity> entities,String prop)
    {
        Set<String> props = new HashSet<>();
        for(Entity e : entities)
        {
            Optional<String> value = e.getStringProp(prop);
            if(value.isPresent())
            {
                props.add(value.get());
            }
        }
        return props;
    }
    private static Reports getEnum(String s)
    {
        Reports report = null;
        for(Reports r : Reports.values()) {
            if (r.name().equals(s.toUpperCase())) {
                report =  r;
            }
        }
        if(report==null)
        {
            System.err.println("Invalid report: "+s);
            System.err.println("Valid reports are:");
            Arrays.stream(Reports.values()).forEach(r->{
                System.err.println(r.name());
            });
            System.exit(1);
        }
        return report;
    }
    enum Reports {
        APPROVED,MULTILYCHII,EARLIEST,DRUGPAGE
    }
    enum Phases {
        APPROVED, WITHDRAWN, PHASEIV, PHASEIII, PHASEII, CLINICAL, PHASEI, PRECLINICAL, BASICRESEARCH, NOTPROVIDED
    }



}
