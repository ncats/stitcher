package ncats.stitcher.tools;

import ncats.stitcher.*;
import ncats.stitcher.Entity;
import ncats.stitcher.domain.*;
import ncats.stitcher.Component;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.springframework.cglib.core.Local;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
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


        if(argv.length<4)
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
        //Iterator<Entity> components= ef.entities("COMPONENT");
        String NAME = "N_Name";
        String PHASE = "HighestPhase";
        String EARLIEST = "EarliestDate";
        String UNII = "I_UNII";
        String LYCHII = "H_LyChI_L3";
        String out ="NAME\tHIGHEST PHASE\tEARLIEST APPROVAL\tACTIVE MOEITY UNIIS\tOTHER UNIIS\tLYCHIIS\n";
        String problem ="";
        Collection<ncats.stitcher.Component> components = ef.components();
        for(Component c : components) {
            c.iterator();
            Entity[] entityArray = c.entities();
            Set<Entity> entities = new HashSet<>();
            Collections.addAll(entities,entityArray);
            Set<String> uniis, lychiis, bestUniis;
            String bestUnii = "NONE";
            String earliest = "NONE";
            String highestPhase = "NOTPROVIDED";

        }
//            while (components.hasNext()) {
//                String name;
//                Set<String> uniis, lychiis, bestUniis;
//                String bestUnii = "NONE";
//                String earliest = "NONE";
//                String highestPhase = "NOTPROVIDED";
//                boolean approved = false;
//                Entity c = components.next();
//                Set<Entity> cluster = c.getCluster();
//                name = c.getStringProp(NAME).get();
//                Optional<LocalDate> earliestDate = findEarliestApprovaleDate(c);
//                if (earliestDate.isPresent()) {
//                    earliest = earliestDate.get().toString();
//                }
//
//
//                bestUniis = uniqueSet(findBestUnii(cluster, graphDb));
//                if (bestUniis.size() > 1) {
//                    bestUnii = "MULTIPLE";
//                    System.out.println(name);
//                    bestUniis.forEach(u -> {
//                        System.out.println(u);
//                    });
//                } else if (bestUniis.isEmpty()) {
//                    //no-op
//                } else {
//                    bestUnii = bestUniis.iterator().next();
//                }
//                for (Entity e : cluster) {
//                    if (isApproved(e)) {
//                        approved = true;
//                        highestPhase = "APPROVED";
//                    }
//
//                }
//                if (!highestPhase.equals("APPROVED")) {
//                    highestPhase = getHighestPhase(cluster).name();
//                }
//                lychiis = uniqueSet(getStringPropBroad(c, LYCHII));
//                uniis = (uniqueSet(getStringPropBroad(c, UNII)));
//                uniis.removeAll(bestUniis);
//                out = out + name + "\t" + highestPhase + "\t" + earliest + "\t" + commaDelimitedOf(bestUniis) + "\t" + commaDelimitedOf(uniis) + "\t" + commaDelimitedOf(lychiis) + "\n";
//                //            if( (highestPhase.equals("APPROVED") ||
//                //                    //!highestPhase.equals("NOTPROVIDED") &&
//                //                    //!highestPhase.equals("CLINICAL") &&
//                //                    highestPhase.equals("WITHDRAWN") ) &&
//                //                    (earliest.equals("NONE")))
//                //            {
//                //                System.out.println(c.getId()+"\t"+name+"\t"+highestPhase+"\t"+earliest);
//                //            }
//                //            if(!bestUnii.equals("NONE"))
//                //            {
//                //                System.out.println(c.getId()+"\t"+name+"\t"+bestUnii);
//                //            }
//            }
//        }
        printLog(out, output);
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
    private void generateEarliestApprovalReport(File output, Set<String>props)
    {
        Map<Entity,LocalDate> dateMap = this.findEarliestApprovalDate(this.findApprovedDrugs());
        String report ="";
        for(Map.Entry<Entity,LocalDate> entry : dateMap.entrySet())
        {
            report = report+entry.getKey().getStringProp("N_Name").get()+"\t"
                    +entry.getValue().toString()+"\n";
        }
        printLog(report,output);
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
    private Map<Entity, LocalDate> findEarliestApprovalDate(Set<Entity> entities)
    {
        Map<Entity, LocalDate> dateMap = new HashMap<>();
        final String EARLIEST = "EarliestDate";

        //TODO should be using Entities arg here
        //Iterator<Entity> components= ef.entities("COMPONENT");
        Iterator<Entity> iter = entities.iterator();
        List<LocalDate> dates = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        while(iter.hasNext())
        {
            Entity e = iter.next();
            Optional<LocalDate> date = findEarliestApprovalDate(e);
            if(date.isPresent())
            {
                dateMap.put(e,date.get());
            }
//            if(e.datasource().getName().equals(FDA))
//            {
//                Set<Set<String>> dateStringSets = e.getPayloadValuesInCluster(EARLIEST);
//                Iterator<Set<String>> outer = dateStringSets.iterator();
//                while(outer.hasNext())
//                {
//                    Set<String> currentSet = outer.next();
//                    Iterator<String> inner = currentSet.iterator();
//                    while(inner.hasNext())
//                    {
//                        String date = inner.next();
//                        if(date.equals("Not given"))
//                        {
//                            continue;
//                        }
//                        try{
//                            dates.add(formatter.parseLocalDate(date));
//                        }
//                        catch(Exception exception)
//                        {
//                            exception.printStackTrace();
//                        }
//                    }
//                }
//            }
            //System.out.println("SIZE: "+dates.size());
//            if(!dates.isEmpty())
//            {
//                Collections.sort(dates);
//                dateMap.put(e,dates.get(0));
//                dates.clear();
//            }

        }


        return dateMap;
    }
    private Optional<LocalDate> findEarliestApprovalDate(Entity e)
    {
        final String EARLIEST = "EarliestDate";
        LocalDate earliest = null;
        List<LocalDate> dates = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");


        Set<Set<String>> dateStringSets = e.getPayloadValuesInCluster(EARLIEST);
        Iterator<Set<String>> outer = dateStringSets.iterator();
        while(outer.hasNext())
        {
            Set<String> currentSet = outer.next();
            Iterator<String> inner = currentSet.iterator();
            while(inner.hasNext())
            {
                String date = inner.next();
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
            Phases current = getHighestPhase(e);
            if(current.ordinal()<highest.ordinal())
            {
                highest=current;
            }
        }
        return highest;
    }
    private Phases getHighestPhase(Entity e)
    {
        String strPhase = "NOTPROVIDED";
        Set<String> highs =e.getPayloadValues("HighestPhase");
        if(highs.size()>1){
            System.err.println(e.getId()+" has multiple HighestPhases?  I don't think you know what highest means Neo4J");
            System.exit(1);
        }
        else if(highs.isEmpty())
        {

            //TODO find the real names for USAN and JAN used in the db
            Set<String> inn = e.getPayloadValues("INN");
            Set<String> usan = e.getPayloadValues("USAN");
            Set<String> jan = e.getPayloadValues("JAN");
            if(!inn.isEmpty()||!usan.isEmpty()||!jan.isEmpty())
            {
                strPhase = "CLINICAL";
            }

        }
        else
        {
            strPhase=highs.iterator().next().toUpperCase().replaceAll("\\s","");
            if(strPhase.equals("NATURALMETABOLITE"))
            {
                strPhase="CLINICAL";
            }
        }
        Phases highest = Phases.valueOf(strPhase);
        return highest;
    }
    private Set<String> findBestUnii(Set<Entity> cluster, GraphDb graphDb)
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
            PrintWriter writer = new PrintWriter(file, "UTF-8");
            writer.println(s);
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
        APPROVED, WITHDRAWN, PHASEIV, PHASEIII,PHASEII,CLINICAL,PHASEI,PRECLINICAL,BASICRESEARCH,NOTPROVIDED
    }



}
