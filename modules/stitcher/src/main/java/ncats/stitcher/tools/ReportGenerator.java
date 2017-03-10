package ncats.stitcher.tools;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;
import ncats.stitcher.*;
import ncats.stitcher.Entity;
import ncats.stitcher.Component;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.*;
import java.util.Comparator;
import java.util.function.Function;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by williamsmard on 2/22/17.
 */
public class ReportGenerator {
    private static final String FDA = "Drugs@FDA";
    private static final String RANCHO = "Rancho 2017";
    private static final String GSRS = "fullSeedData-2016-06-16.gsrs";

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
        File file = new File(argv[1]+sdf.format(timestamp)+".txt");
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
        String NAME = "N_Name";
        String PHASE = "HighestPhase";
        String EARLIEST = "EarliestDate";
        String UNII = "I_UNII";
        String LYCHII = "H_LyChI_L3";
        //String out ="NAME\tHIGHEST PHASE\tEARLIEST APPROVAL\tACTIVE MOEITY UNIIS\tOTHER UNIIS\tLYCHIIS\n";
        String problem ="";

        Collection<ncats.stitcher.Component> components = ef.components();
        int has = 0;
        int hasNot = 0;
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

            for (Entity e : entities) {
                if (isApproved(e)) {
                    highestPhase = "APPROVED";
                    break;
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
            if(!mols.isEmpty())
            {
                has++;
                try{
                    Molecule smallest = mols.stream().min((o1,o2)->o1.getAtomCount()-o2.getAtomCount()).get();
                    smallest.setProperty("name",name);
                    smallest.setProperty("highestPhase",highestPhase);
                    smallest.setProperty("earliestApproval",earliest);
                    smallest.setProperty("activeUniis",commaDelimitedOf(activeUniis));
                    smallest.setProperty("otherUniis",commaDelimitedOf(uniis));
                    smallest.setProperty("lychiis",commaDelimitedOf(lychiis));
                    //System.out.println(smallest.toFormat("sdf"));
                    out=out+smallest.toFormat("sdf");

                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }


            }
            else
            {
                Molecule blank = new Molecule();
                blank.setProperty("name",name);
                blank.setProperty("highestPhase",highestPhase);
                blank.setProperty("earliestApproval",earliest);
                blank.setProperty("activeUniis",commaDelimitedOf(activeUniis));
                blank.setProperty("otherUniis",commaDelimitedOf(uniis));
                blank.setProperty("lychiis",commaDelimitedOf(lychiis));
                out=out+blank.toFormat("sdf");

            }
            printLog(out,output);
            //out = out + name + "\t" + highestPhase + "\t" + earliest + "\t" + commaDelimitedOf(activeUniis) + "\t" + commaDelimitedOf(uniis) + "\t" + commaDelimitedOf(lychiis) + "\n";
        }
    }
    private String bestName(Set<Entity> entities)
    {
        for(Entity e : entities)
        {
            Optional<String> name = e.getStringProp("N_NAME");
            Optional<String> moiety = e.getStringProp("T_ActiveMoiety");
            Optional<String> unii = e.getStringProp("I_UNII");

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
    private Optional<Entity> getMoiety(Set<Entity> entities)
    {
        Set<Entity> trueMoieties = new HashSet<>();
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
                        trueMoieties.add(am.get());
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
        if(!otherMoieties.isEmpty() || !trueMoieties.isEmpty())
        {
            if(trueMoieties.size()>1)
            {
                System.out.println("HAS MULTIPLE TRUE MOIETIES");
                System.out.println(entities.iterator().next().getId()+" "+name+" "+trueMoieties.size()+" "+otherMoieties.size());
            }
            if(!otherMoieties.isEmpty())
            {
                System.out.println("HAS FALSE MOIETIES");
                System.out.println(entities.iterator().next().getId()+" "+name+" "+trueMoieties.size()+" "+otherMoieties.size());
            }
        }

        return Optional.ofNullable(moiety);
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
    //TODO Refactor this method to work again
   private void generateEarliestApprovalReport(File output, Set<String>props)
    {
//        Map<Entity,LocalDate> dateMap = this.findEarliestApprovalDate(this.findApprovedDrugs());
//        String report ="";
//        for(Map.Entry<Entity,LocalDate> entry : dateMap.entrySet())
//        {
//            report = report+entry.getKey().getStringProp("N_Name").get()+"\t"
//                    +entry.getValue().toString()+"\n";
//        }
//        printLog(report,output);
        System.err.println("THIS REPORT WAS MADE A NO-OP BECAUSE MARK HASN'T REFACTORED IT YET");
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
        final String EARLIEST = "EarliestDate";
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
