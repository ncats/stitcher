package ncats.stitcher.calculators.events;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class WithdrawnEventParser extends EventParser{

    private static class WithdrawnInfo{
        private final List<String> countriesWithdrawn;
        private final String reason;
        //TODO add other fields like date withdrawn


        public WithdrawnInfo(String reason, List<String> countriesWithdrawn) {
            this.countriesWithdrawn = countriesWithdrawn;
            this.reason = reason;
        }

        public List<String> getCountriesWithdrawn() {
            return countriesWithdrawn;
        }

        public String getReason() {
            return reason;
        }
    }
    private static class WithdrawnStatusLookup{
        private final Map<String, WithdrawnInfo> withdrawnsById = new HashMap<>();

        private static Pattern COL_PATTERN = Pattern.compile("\t");
        private static Pattern COUNTRY_PATTERN = Pattern.compile("\\|");

        WithdrawnStatusLookup(InputStream in) throws IOException{
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(in))){
                String[] headerArray = COL_PATTERN.split(reader.readLine());
                Map<String, Integer> header = new HashMap<>();
                for( int i=0; i< headerArray.length; i++){
                    header.put(headerArray[i], i);
                }
                //status	generic_name	brand_name	synonym	form	class	moa	indication	description	year_launched	date_launched	country_launched	date_shortage	year_shortage	URL	year_adr_report	year_withdrawn	date_withdrawn	country_withdrawn	reason_withdrawn	year_remarketed	source	unii	smiles

                int uniiOffset = header.get("unii");
                int reasonWithDrawn = header.get("reason_withdrawn");
                int status = header.get("status");
                int countryWithdrawn = header.get("country_withdrawn");


                Map<String, List<String>> cache = new HashMap<>();
                String line;
                while( (line=reader.readLine()) !=null){
                    String[] cols = COL_PATTERN.split(line);

                    if("Withdrawn".equals(cols[status])){
                        String id = cols[uniiOffset];
                        withdrawnsById.put(id, new WithdrawnInfo(
                                cols[reasonWithDrawn],
                                cache.computeIfAbsent(cols[countryWithdrawn], k-> Arrays.asList(COUNTRY_PATTERN.split(k)))));


                    }
                }
            }
        }

        public WithdrawnInfo getWithdrawnInfo(String unii){
            return withdrawnsById.get(unii);
        }
    }
    static WithdrawnStatusLookup withdrawnStatusLookup;
    static{

        try{
            withdrawnStatusLookup = new WithdrawnStatusLookup(new BufferedInputStream(new FileInputStream("data/combined_withdrawn_shortage_drugs.txt")));
        }catch(IOException e) {

            throw new UncheckedIOException(e);
        }
    }

    public WithdrawnEventParser() {
        super("combined_withdrawn_shortage_drugs.txt");
    }

    @Override
    public List<Event> getEvents(Map<String, Object> payload) {

        List<Event> events = new ArrayList<>();
        String unii = (String) payload.get("unii");

        if("0QTW8Z7MCR".equals(unii)){
            System.out.println("MERGED PAYLOAD = \n" + payload);
        }
        WithdrawnInfo info = withdrawnStatusLookup.getWithdrawnInfo(unii);
        if(info !=null){
            Event e = new Event(name, unii, Event.EventKind.Withdrawn);
            e.comment = info.getReason();

            if(info.countriesWithdrawn.isEmpty()){
                events.add(e);
            }else {
                for (String country : info.getCountriesWithdrawn()) {
                    Event e2 = e.clone();
                    e2.jurisdiction = country;
                    events.add(e2);
                }
            }
        }


        return events;
    }
}
