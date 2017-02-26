package ncats.stitcher.tools;

import ncats.stitcher.*;
import ncats.stitcher.Entity;

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

        while(ei.hasNext())
        {
            count++;
            String line = "";
            Entity entity = ei.next();
            Iterator<String> pi = props.iterator();
            while(pi.hasNext())
            {
                String prop = pi.next();
                Set<String> propVals = getStringPropBroad(entity,prop);
                if(propVals.size()>1)
                {
                    StringBuilder sb = new StringBuilder();
                    for(String p : propVals)
                    {
                        sb.append(p).append(",");
                    }
                    line = line+sb.deleteCharAt(sb.length() - 1).toString()+"\t";
                    //line = line+"MULTIPLE_VALUES"+"\t";
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
            }
            output = output+line+"\n";
        }
        //System.out.println(output);
        printLog(output,file);

    }
    public static Set<String> getStringPropBroad(Entity e, String s)
    {
        Set<Entity> hasProp = new HashSet<Entity>();
        Set<String> propValues = new HashSet<String>();
        if(e.properties().containsKey(s))
        {
            getStringProp(e,s).ifPresent(prop->{
                propValues.add(prop);
            });
        }
        else
        {
            Entity[] neighbors = e.neighbors();
            for(Entity neighbor:neighbors)
            {
                if(neighbor.properties().containsKey(s))
                {
                   if(getStringProp(neighbor,s).isPresent())
                   {
                       propValues.add(getStringProp(neighbor,s).get());
                   }

                }
            }
        }
        return propValues;

    }
    public static Optional<String> getStringProp(Entity e, String s)
    {
        Optional<String> value = Optional.empty();
        if(e.properties().containsKey(s))
        {
            //System.out.println(e.getId()+" has key "+s);
            Object objVal = e.properties().get(s);
            if(objVal instanceof  String)
            {
                value = Optional.of((String)objVal);
            }
            else if(objVal instanceof Object[])
            {
                String[] valueList = (String[])objVal;
                StringBuilder sb = new StringBuilder();
                if(valueList[0] instanceof String)
                {
                    for(String val : valueList)
                    {
                        sb.append(val).append(",");
                    }
                    value = Optional.of(sb.deleteCharAt(sb.length() - 1).toString());

                }
            }
        }
        return value;
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
        APPROVED,MULTILYCHII
    }



}
