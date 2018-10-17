package ncats.stitcher.calculators;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ncats.stitcher.*;
import ncats.stitcher.calculators.events.Event;
import ncats.stitcher.calculators.summary.JsonNodeMaker;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import play.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ncats.stitcher.calculators.summary.Tuple;
import ncats.stitcher.calculators.summary.UtilInxight;
import ncats.stitcher.calculators.summary.UtilInxight.Final;

public class SummaryEventCalculator extends StitchCalculator {

    final DataSourceFactory dsf;
    private static final String APPROVAL_YEAR = "Approval Year";
    private static final String IS_HIGHEST_PHASE_APPROVED = "Is Highest Phase Approved";
    public static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Stitch.class.getName());

    private ObjectMapper objectmapper = new ObjectMapper();

    enum DEV_STATUSES {
        WITHDRAWN("US Withdrawn", "Withdrawn", 0),
        APPROVED_OTC("US Approved OTC", "USApprovalOTC", 1),
        APPROVED_RX("US Approved Rx", "USApprovalRx", 2),
        MARKETED("US Unapproved, Marketed", "Marketed", 3),
        //  MARKETED("Marketed","Marketed",2),
        CLINICAL("Clinical", "ClinicalTrial", 4),
        OTHER("Other", "Other", 5);

        private static final Map<String, DEV_STATUSES> byDisplayName;
        private static final Map<String, DEV_STATUSES> byStitcherName;

        static {
            byDisplayName = new HashMap<>();
            byStitcherName = new HashMap<>();
            for (DEV_STATUSES p : values()) {
                byDisplayName.put(p.displayName, p);
                byStitcherName.put(p.stitcherName, p);
            }
        }

        private final String displayName;
        private final String stitcherName;
        private final int order;

        DEV_STATUSES(String displayName, String stitchName, int order) {
            this.displayName = displayName;
            this.stitcherName = stitchName;
            this.order = order;
        }

        public static DEV_STATUSES parse(String displayName) {
            return byDisplayName.get(displayName);
        }

        public static DEV_STATUSES fromStitcher(String stitcherName) {

            DEV_STATUSES stat = byStitcherName.get(stitcherName);
            if (stat == null) {
                logger.info("Couldn't find:" + stitcherName);
                stat = DEV_STATUSES.OTHER;
            }
            return stat;
        }

        public String displayName() {
            return this.displayName;
        }

        public int getOrder() {
            return this.order;
        }
    }

    enum PHASES {
        NOT_PROVIDED("Not Provided"),
        PHASE_I("Phase I"),
        PHASE_II("Phase II"),
        PHASE_III("Phase III"),
        PHASE_IV("Phase IV"),
        APPROVED_OFF_LABEL("Approved (off-label)"),
        APPROVED("Approved");

        private static final Map<String, PHASES> byDisplayName;

        static {
            byDisplayName = new HashMap<>();
            for (PHASES p : values()) {
                byDisplayName.put(p.displayName, p);
            }
        }

        private final String displayName;

        PHASES(String displayName) {
            this.displayName = displayName;
        }

        public static PHASES parse(String displayName) {
            return byDisplayName.get(displayName);
        }

        public static Optional<PHASES> parse(JsonNode node) {
            JsonNode phase = node.at("/highestPhase");
            if (phase != null && !phase.isMissingNode()) {
                return Optional.ofNullable(PHASES.parse(phase.asText()));
            }

            return Optional.empty();
        }
    }

    public SummaryEventCalculator(EntityFactory ef) {
        this.ef = ef;
        this.dsf = ef.getDataSourceFactory();
    }

    /*
     * Okay, this is the logic we're going to use:
     * 
     * There is one Development Status facet, with the following possible values:
     * 
     * 1. Approved [US Rx and OTC]
     * 2. Marketed (Non-US Approved)
     * 3. Clinical 
     * 4. Other
     * 
     * 
     * You can further break this down if you want:
     * 
     * 1. US Approved Rx
     * 2. US Approved OTC
     * 3. Marketed
     * 4. 
     * 
     * 
     * (non-Javadoc)
     * @see ix.core.additional.AdditionalDataFinder#getAdditionalDataFor(ix.core.util.EntityUtils.Key)
     */


    static protected class DevStatusEvent {

        @JsonIgnore
        private DEV_STATUSES devStatus;


        //e.g. "NDAXXXXX"
        public String sourceID;
        public String sourceURL;

        public String year = null;


        @JsonProperty("status")
        public String getStatus() {
            return devStatus.displayName();
        }

        public DevStatusEvent(DEV_STATUSES dev, String sourceID, String sourceURL) {
            this.devStatus = dev;
            this.sourceID = sourceID;
            this.sourceURL = sourceURL;
        }

        public DevStatusEvent year(String year) {
            this.year = year;
            return this;
        }

        public static DevStatusEvent from(DEV_STATUSES status, String sourceID, String sourceURL) {
            return new DevStatusEvent(status, sourceID, sourceURL);
        }

        public boolean isApproved() {

            return this.devStatus.displayName().contains("Approved");
        }

        public boolean isWithdrawn() {
            return this.devStatus == DEV_STATUSES.WITHDRAWN;
        }

        public boolean isCertainlyBefore(DevStatusEvent devStatusEvent) {
            if (this.year == null || devStatusEvent.year == null) {
                return false;
            }
            if (this.getYearAsInt() < devStatusEvent.getYearAsInt()) {
                return true;
            }
            return false;
        }

        @JsonIgnore
        public int getYearAsInt() {
            if (this.year == null) return 0;
            return Integer.parseInt(this.year);
        }

        public boolean isCertainlyAfter(DevStatusEvent devStatusEvent) {
            if (this.year == null || devStatusEvent.year == null) {
                return false;
            }
            if (this.getYearAsInt() > devStatusEvent.getYearAsInt()) {
                return true;
            }
            return false;
        }
    }


    public static class StitcherEvent implements Comparable<StitcherEvent> {

        private static Comparator<String> sourceOrder = UtilInxight.comparatorItems("approvalYears.txt");

        public String kind;
        //public String id;
        public String source;
        public String date;
        public long created;
        public String jurisdiction;
        public String comment;
        public Integer withdrawn_year = null;

        //spl vars start
        public String ApprovalAppId;
        public String MarketingStatus;
        public String ProductCategory;
        public String NDC;
        public String URL;
        //spl vars end

        public DevStatusEvent toDevStatusEvent() {
            DEV_STATUSES stat = DEV_STATUSES.fromStitcher(kind);

            String sourceID = null;
            String sourceURL = null;

            //(see below) somehow if NDC is simply reassign its value + X,
            //X is repeated multiple times
            String refNDC = null;

/*          prepare something like that for all spl entries
            21 CFR 356 - OTC monograph not final
            referenced by NDC: <a href=_dailymedurl_>7770001-1232</a>
*/
            //try preparing source ID for all spl events
            if (source.startsWith("spl_")) {
                if (NDC != null) {
                    refNDC = "referenced by NDC: " + NDC;
                }

                sourceID = Stream.of(ApprovalAppId, MarketingStatus, refNDC)
                        .filter(s -> s != null && !s.isEmpty())
                        .collect(Collectors.joining(" - "));
                sourceURL = URL;
            } else if (comment != null) {
                Tuple<String, List<String>> ext = UtilInxight.extractUrls(comment);
                sourceID = ext.k();
                sourceURL = ext.v().stream().findFirst().orElse(null);
            }


            DevStatusEvent de = DevStatusEvent.from(stat, sourceID, sourceURL);

            if (this.date != null) {
                return de.year(date.split("-")[0]);
            } else if (this.withdrawn_year != null) {
                return de.year(withdrawn_year + "");
            }
            return de;
        }

        public boolean isUncertainDate() {
            if (this.comment != null && this.comment.contains("Uncertain")) {
                return true;
            }
            return false;
        }

        public String getEffectiveYear() {
            if (this.date != null) {
                return date.split("-")[0];
            } else if (this.withdrawn_year != null) {
                return withdrawn_year + "";
            }
            return null;
        }

        public int getEffectiveOrder() {
            if (this.toDevStatusEvent().isWithdrawn()) {
                if (!"US".equals(this.jurisdiction)) {
                    return 100;
                }
            }
            return this.toDevStatusEvent().devStatus.getOrder();

        }

        public boolean isUS() {
            if ("US".equals(this.jurisdiction)) return true;
            return false;
        }

        //Positive if o should come before

        @Override
        public int compareTo(StitcherEvent other) {
            DevStatusEvent dev1 = this.toDevStatusEvent();
            DevStatusEvent dev2 = other.toDevStatusEvent();

            // Initial Order:
            //  0: 	US WITHDRAWN	  [unless an approval event in the US happened AFTER]
            //  1:  APPROVED_OTC
            //  2:  APPROVED_RX
            //  3:  MARKETED
            //  4:	CLINICAL
            //  5:  OTHER
            //100:  WITHDRAWN NON-US
            //Tie Breakers: if 2 events are tied:
            //   a. The one from the most trusted source is first (Orange Book + Drugs @ FDA)
            //   b. If still tied, and only 1 has a startDate, return that one first
            //   c. If still tied, and both have a startDate, return the older event first

            int o1 = this.getEffectiveOrder();
            int o2 = other.getEffectiveOrder();


            int ord = o1 - o2;

            //If they're both US
            if (this.isUS() && other.isUS()) {
                if (dev1.isApproved() && dev2.isWithdrawn()) {
                    if (dev2.isCertainlyAfter(dev1)) {
                        return 1;
                    } else if (dev2.isCertainlyBefore(dev1)) {
                        return -1;
                    }
                }

                if (dev2.isApproved() && dev1.isWithdrawn()) {
                    if (dev1.isCertainlyAfter(dev2)) {
                        return -1;
                    } else if (dev1.isCertainlyBefore(dev2)) {
                        return 1;
                    }
                }
            }


            if (ord == 0) {
                String s1 = this.source;
                String s2 = other.source;
                if (this.isUncertainDate()) {
                    s1 = s1 + "UncertainDate";
                }
                if (other.isUncertainDate()) {
                    s2 = s2 + "UncertainDate";
                }
                ord = sourceOrder.compare(s1, s2);
                if (ord == 0) {

                    if (this.getEffectiveYear() != null && other.getEffectiveYear() != null) {
                        ord = this.getEffectiveYear().compareTo(other.getEffectiveYear());
                    } else {
                        if (this.getEffectiveYear() == null && other.getEffectiveYear() != null) {
                            ord = 1;
                        } else if (this.getEffectiveYear() != null && other.getEffectiveYear() == null) {
                            ord = -1;
                        }
                    }
                }
            }

            return ord;
        }

    }

    //public Stream<AdditionalData<?, ?>> getAdditionalDataFor(Key k) {
    public Stream<Object> OldCode() {
        try {

            //This is used for the full record status
            Final<DevStatusEvent> recordStatus = Final.of(DevStatusEvent.from(DEV_STATUSES.OTHER, null, null));

            Final<DevStatusEvent> earliestApproved = Final.of(null);
            Final<String> substanceForm = Final.of("Salts, Hydrates, Esters, etc.");


            final Consumer<DevStatusEvent> devStatusUpdater = (rs) -> {
                int olds = recordStatus.get().devStatus.getOrder();
                int news = rs.devStatus.getOrder();

                //System.out.println("Found:" + rs.devStatus.displayName());


                if (news < olds) {
                    recordStatus.set(rs);
                }
                if (rs.isApproved()) {
                    DevStatusEvent old = earliestApproved.get();
                    //If the old approval is null or has no year, use this one
                    if (old == null || old.year == null) {
                        earliestApproved.set(rs);

                        //Otherwise, use it only if the year is definitely before the old startDate
                    } else if (rs.isCertainlyBefore(earliestApproved.get())) {
                        earliestApproved.set(rs);
                    }
                }
            };

            // TODO Check if substance has INN or USAN name which implies CLINICAL status
//            return (Stream<AdditionalData<?, ?>>) k.fetch().map(v -> {
//                Substance s = (Substance) v.getValue();
//                List<AdditionalData<?, ?>> additionalData = new ArrayList<>();
//
//                SortedSet<PHASES> seenPhases = new TreeSet<>();
//
//
//                //Very hacky way to get Clinical Candidates
//                boolean isINNUSANClinical = s.tags.stream()
//                        .filter(kw -> kw.getValue().equals("INN") || kw.getValue().equals("USAN"))
//                        .findAny().isPresent();
//                if (!isINNUSANClinical) {
//                    isINNUSANClinical = s.references.stream()
//                            .filter(sref -> (sref.citation + "").contains("[INN]") || (sref.citation + "").contains("[USAN]"))
//                            .findAny()
//                            .isPresent();
//
//                    if (!isINNUSANClinical) {
//                        isINNUSANClinical = s.names.stream()
//                                .map(nm -> nm.getName())
//                                .filter(nm -> nm.contains("[INN]") || nm.contains("[USAN]"))
//                                .findAny()
//                                .isPresent();
//
//                    }
//                }
//
//                if (isINNUSANClinical) {
//                    //TODO: Get INN list, and year
//                    devStatusUpdater.accept(DevStatusEvent.from(DEV_STATUSES.CLINICAL, "USAN/INN", null));
//                }

                // TODO get StitchNode
//                if (s.getApprovalID() != null) {
//
//                    JsonNode jsn = new StitcherResolver().resolve(s.getApprovalID());
//
////                System.out.println(jsn);
//
//                    if (jsn != null && !jsn.isMissingNode() && !jsn.isNull()) {

                // TODO look for phase from Broad Source
//                        JsonNode broadphaseNode = jsn.at("/sgroup/properties/clinical_phase");
//
//
//                        if (!broadphaseNode.isMissingNode()) {
//
//                            Consumer<JsonNode> process = (broadPhase) -> {
//                                String p = broadPhase.at("/value").asText();
//                                if (p == null || p.equals("")) {
//                                    additionalData.add(BasicStringAdditionalData
//                                            .from(k, "Broad Phase", "missing", true));
//                                } else {
//                                    additionalData.add(BasicStringAdditionalData
//                                            .from(k, "Broad Phase", p, true));
//                                }
//                            };
//
//                            if (broadphaseNode.isArray()) {
//                                for (JsonNode broadPhase1 : broadphaseNode) {
//                                    process.accept(broadPhase1);
//                                }
//                            } else {
//                                process.accept(broadphaseNode);
//                            }
//
//                        }

                        // TODO Extract Labels, Patents, PMIDs, CNS, etc.
//                        for (JsonNode jsval : jsn.at("/labels")) {
//                            additionalData.add(BasicStringAdditionalData
//                                    .from(k, "Stitcher Label", jsval.asText(), true));
//                        }
//
//
//                        List<Tuple<String, String>> facets = new ArrayList<>();
//
//                        List<JsonNodeAdditionalData> conditions = new ArrayList<JsonNodeAdditionalData>();
//                        List<JsonNodeAdditionalData> targets = new ArrayList<JsonNodeAdditionalData>();
//
//                        facets.add(Tuple.of("Substance Form", "/stitcherType"));
//
//                        Optional.ofNullable(jsn.at("/stitcherType").asText())
//                                .filter(t -> !t.equals(""))
//                                .ifPresent(t -> {
//                                    substanceForm.set(t);
//                                });
//
//                        additionalData.addAll(JsonNodeAdditionalData.from(k, "Stitcher Info", jsn, facets).decompose().collect(Collectors.toList()));
//
//                        forEachArrayValue(jsn, "/sgroup/properties/Pmids", n -> additionalData.add(PubmedAdditionalData.from(k, n.at("/value").asText())));
//
//                        forEachArrayValue(jsn, "/sgroup/properties/CompoundPatent", n -> additionalData.add(BasicStringAdditionalData.from(k, "Patent", n.at("/value").asText(), true)));
//
//                        extractDescription(jsn).ifPresent(n -> additionalData.add(JsonNodeAdditionalData.from(k, "Description", n)));
//
//                        extractCns(jsn).ifPresent(n -> additionalData.add(JsonNodeAdditionalData.from(k, "CNS Activity", n, Collections.singletonList(Tuple.of("CNS Activity", "/cns")))));
//
//                        extractOriginator(jsn).ifPresent(n -> additionalData.add(JsonNodeAdditionalData.from(k, "Originator", n, Collections.singletonList(Tuple.of("Originator", "/originator")))));


                        // TODO: Parse the events directly
                    
                    /*
                     
                     [
{
kind: "USApprovalRx",
id: "63020-078",
source: "spl_acti_rx.txt",
startDate: "2015-11-20",
created: 1509266743019,
jurisdiction: "US",
comment: "NDA208462"
},
{
kind: "USApprovalRx",
source: "approvalYears.txt",
id: "46CWK97Z3K",
startDate: "2015-11-20",
created: 1509266743019,
jurisdiction: "US",
comment: "GOT FROM DRUGS@FDA + OB sep 13 2017"
}
                     */
//                        try {
//                            List<StitcherEvent> events = StreamUtil.forIterable(jsn.at("/events"))
//                                    .map(jso -> Unchecked.uncheck(() -> (StitcherEvent) objectmapper.readValue(jso.toString(), StitcherEvent.class)))
//                                    .sorted()
//                                    .collect(Collectors.toList());
//
//                            if (!events.isEmpty()) {
//                                //System.out.println(s.getApprovalID() + "::" + events.get(0).toDevStatusEvent().devStatus.displayName());
//                                devStatusUpdater.accept(events.get(0).toDevStatusEvent());
//                            }
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//
//
//                        Set<String> npcAnnotations = StreamUtil.forIterable(jsn.at("/sgroup/properties/DATASET"))
//                                .map(v1 -> v1.at("/value").asText())
//                                .collect(Collectors.toSet());
//                        if (!npcAnnotations.isEmpty()) {
//                            ObjectNode on2 = new ObjectMapper().createObjectNode();
//                            on2.put("npc", jsn.at("/sgroup/properties/DATASET"));
//                            additionalData.add(JsonNodeAdditionalData.from(k, "NPC Datasets", on2, Arrays.asList(
//                                    Tuple.of("NPC Dataset", "/npc/*/value")
//                            )));
//                        }

                        // TODO Parse target info
                        //targets is base64 encoded json?

//                    System.out.println(jsn);
//                        JsonNode targetNode = jsn.at("/sgroup/properties/Targets");
//                        if (!targetNode.isMissingNode()) {
//                            //there might be several
//                            for (JsonNode targetElementNode : targetNode) {
////                        System.out.println("target json = " + extractTarget(jsn.at("/sgroup/properties/Targets").get(0).at("/value").asText()));
//                                JsonNodeAdditionalData targetAdditionalData = JsonNodeAdditionalData.from(k, "Target Info",
//                                        extractTarget(targetElementNode.at("/value").asText()),
//                                        Arrays.asList(Tuple.of("Primary Target", "/label"), Tuple.of("Pharmacology", "/pharmacology")));
//
//                                targetAdditionalData.addNewIndexer((add, consumer) -> {
//                                    JsonNode jsn2 = add.getValue();
//
//                                    String tar = jsn2.at("/label").asText();
//                                    String phar = jsn2.at("/pharmacology").asText();
//
//                                    if (tar.length() > 0 && phar.length() > 0) {
//                                        consumer.accept(IndexableValue.simpleFacetStringValue("Target Pharmacology", tar + " " + phar.toLowerCase()));
//                                    }
//
//                                });
//
//                                targets.add(targetAdditionalData);
//                            }
//                        }

                        // TODO Parse condition info
//                        JsonNode conditionNode = jsn.at("/sgroup/properties/Conditions");
//
//                        AtomicBoolean alreadyFacetedHighestPhase = new AtomicBoolean(false);
//
//                        if (!conditionNode.isMissingNode()) {
//                            for (JsonNode conditionElement : conditionNode) {
//                                String conditionsBase64 = conditionElement.at("/value").asText();
//                                extractConditions(conditionsBase64)
//                                        .ifPresent(conditionJsn -> {
//                                            PHASES.parse(conditionJsn).ifPresent(phase -> seenPhases.add(phase));
//                                            conditions.add(JsonNodeAdditionalData.from(k, "Condition Info", conditionJsn,
//                                                    Arrays.asList(Tuple.of("Condition", "/label"),
//                                                            Tuple.of("Treatment Modality", "/modality"),
////                                                Tuple.of("Highest Phase", "/highestPhase"),
//                                                            //Tuple.of(IS_HIGHEST_PHASE_APPROVED, "/isHighestPhaseApproved"),
//                                                            Tuple.of("Approval Source", "/approvalSource"),
//                                                            Tuple.of("Is Clinical Candidate", "/isClinical")
//
//                                                    )));
//                                        });
//
//                            }
//                            StreamUtil.forIterable(conditionNode)
//                                    .map(e -> e.at("/value").asText())
//                                    .map(jsn1 -> extractOfflabelConditions(jsn1))
//                                    .filter(op -> op.isPresent())
//                                    .map(op -> op.get())
//                                    .map(jsn1 -> {
//                                        PHASES.parse(jsn1).ifPresent(phase -> seenPhases.add(phase));
//
//                                        return JsonNodeAdditionalData.from(k, "Condition Info", jsn1,
//                                                Arrays.asList(Tuple.of("Condition", "/label"),
//                                                        Tuple.of("Treatment Modality", "/modality"),
////                                                      Tuple.of("Highest Phase", "/highestPhase"),
//                                                        Tuple.of("Off-Label Condition", "/label")));
//                                    })
//                                    .map(ol -> Tuple.of(ol.getAt("/label").asText(), ol).withKEquality())
//                                    .distinct()
//                                    .map(t -> t.v())
//                                    .forEach(c -> {
//                                        conditions.add(c);
//                                    });
//
//
//                        }

                        // TODO Highest phase
//                        if (!alreadyFacetedHighestPhase.get() && !seenPhases.isEmpty()) {
//                            PHASES highestSeenPhase = seenPhases.last();
//                            ObjectNode on = new ObjectMapper().createObjectNode();
//                            on.put("highestPhase", highestSeenPhase.displayName);
//                            additionalData.add(JsonNodeAdditionalData.from(k, "Stitcher Highest Phase", on, Arrays.asList(
//                                    Tuple.of("Highest Phase", "/highestPhase"))));
//                        }

                        // TODO Condition, Target info
//                        Map<String, String> targetMap = new HashMap<>();
//
//                        for (JsonNodeAdditionalData n : targets) {
//                            targetMap.put(n.getValue().at("/StitcherId").asText(), n.getValue().at("/label").asText());
//                        }
//                        Map<String, String> conditionMap = new HashMap<>();
//
//                        for (JsonNodeAdditionalData n : conditions) {
//                            conditionMap.put(n.getValue().at("/StitcherId").asText(), n.getValue().at("/label").asText());
//                        }
//
//                        for (JsonNodeAdditionalData n : targets) {
//                            ArrayNode ar = ((ObjectNode) n.getValue()).putArray("conditions");
//
//                            for (JsonNode condition : n.getValue().at("/ConditionRefs")) {
//                                ar.add(conditionMap.get(condition.asText()));
//                            }
//
//
//                        }
//
//                        for (JsonNodeAdditionalData n : conditions) {
//                            ArrayNode ar = ((ObjectNode) n.getValue()).putArray("targets");
//
//                            for (JsonNode t : n.getValue().at("/TargetRefs")) {
//                                ar.add(targetMap.get(t.asText()));
//                            }
//
//                        }
//
//
//                        additionalData.addAll(targets);
//                        additionalData.addAll(conditions);

                    // TODO Sample Use Guide
//"Sample Use Guides", val, "{\"In Vivo\":\"/inVivoRoute\"}"});
//                        extractUseGuide(jsn.at("/sgroup/properties"))
//                                .ifPresent(node -> additionalData.add(JsonNodeAdditionalData.from(k, "Sample Use Guides",
//                                        node,
//                                        Collections.singletonList(Tuple.of("In Vivo", "/InVivoRoute")))));
//
//
//                    }
//                }

                // TODO Calculate Earliest Approval
//                DevStatusEvent rstatus = recordStatus.get();
//
//                if (earliestApproved.get() != null) {
//                    String y = earliestApproved.get().year;
//                    if (y != null) {
//                        additionalData.add(BasicStringAdditionalData
//                                .from(k, "Approval Year", y, true));
//                        additionalData.add(BasicStringAdditionalData
//                                .from(k, "sApproval Year", y, true));
//                    } else {
//                        additionalData.add(BasicStringAdditionalData
//                                .from(k, "Approval Year", "Unknown", true));
//                    }
//                }
//
//                additionalData.add(JsonNodeAdditionalData.from(k, "Highest Development Event",
//                        EntityWrapper.of(rstatus).toFullJsonNode(),
//                        Collections.singletonList(Tuple.of("Development Status", "/status"))));
//
//                if (rstatus != null) {
//                    int dorder = rstatus.devStatus.order;
//
//
//                    if (rstatus.devStatus.equals(DEV_STATUSES.APPROVED_OTC)) {
//                        //dorder;
//                    } else if (rstatus.devStatus.equals(DEV_STATUSES.APPROVED_RX)) {
//                        dorder -= 2;
//                    } else if (rstatus.devStatus.equals(DEV_STATUSES.WITHDRAWN)) {
//                        dorder += 2;
//                    }
//                    additionalData.add(BasicStringAdditionalData
//                            .from(k, "sDevelopment Status", "A_" + dorder, true));
//
//                }
//
//                additionalData.add(BasicStringAdditionalData
//                        .from(k, "sSubstance Form", substanceForm.get(), true));
//
//                return additionalData.stream();
//            })
//                    .orElse(Stream.empty());
        } catch (Exception e) {
            Logger.error(e.getMessage(), e);
            e.printStackTrace();
            return Stream.empty();
        }
        return null;
    }


    private static void makeConditionFacets(JsonNode node) {

        JsonNode phase = node.at("/highestPhase");
        if (phase != null && !phase.isMissingNode()) {
            PHASES phases = PHASES.parse(phase.asText());
            if (phases != null) {

            }
        }

        /*
         conditions.add(JsonNodeAdditionalData.from(k, "Condition Info", conditionJsn,
                                                Arrays.asList(Tuple.of("Condition", "/label"),
                                                Tuple.of("Treatment Modality", "/modality"),
                                                Tuple.of("Highest Phase", "/highestPhase"),
                                                //Tuple.of(IS_HIGHEST_PHASE_APPROVED, "/isHighestPhaseApproved"),
                                                Tuple.of("Approval Source", "/approvalSource"),
                                                Tuple.of("Is Clinical Candidate", "/isClinical")

                                        		)));
                                    });

                        }
                        StreamUtil.forIterable(conditionNode)
                                  .map(e-> e.at("/value").asText())
                                  .map(jsn1->extractOfflabelConditions(jsn1))
                                  .filter(op->op.isPresent())
                                  .map(op->op.get())
                                  .map(jsn1->{
                                	  return JsonNodeAdditionalData.from(k, "Condition Info", jsn1,
                                              Arrays.asList(Tuple.of("Condition", "/label"),
                                                      Tuple.of("Treatment Modality", "/modality"),
                                                      Tuple.of("Highest Phase", "/highestPhase"),
                                                      Tuple.of("Off-Label Condition", "/label")));
         */
    }

/*
    private static Stream<JsonNodeAdditionalData> collapseJsonKVNodes(Stream<JsonNodeAdditionalData> s, String groupByJSONPointer, String pivotJSONPointer) {
        return s.map(kve -> Tuple.of(Unchecked.uncheck(() -> new ObjectMapper().readTree(
                kve.getValue().toString()))
                , kve))


                .collect(Collectors.groupingBy(t -> t.k().at(groupByJSONPointer).asText()))
                .entrySet()
                .stream()
                .map(Tuple::of)
                .map((t) -> {
                    JsonNodeAdditionalData kv = t.v().get(0).v();
                    JsonNode js1 = t.v().get(0).k();

                    List<String> conditions =
                            t.v().stream()
                                    .map(c -> c.k().at(pivotJSONPointer))
                                    .filter(c -> c != null && !c.isMissingNode())
                                    .map(c -> c.asText())
                                    .collect(Collectors.toList());


                    ArrayNode arr = ((ObjectNode) js1).putArray(pivotJSONPointer.substring(1));

                    for (String c : conditions) {
                        arr.add(c);
                    }


                    kv.setValue(js1);
                    return kv;
                });
    }
*/

    private Optional<JsonNode> extractOfflabelConditions(String base64Encoded) {
        try {
            JsonNode jsn = new JsonNodeMaker(new ObjectMapper().readTree(Base64.getDecoder().decode(base64Encoded)))
                    .add("OfflabelUse", "label")
                    .addList("OfflabelUseUri", "uri", "\\|")
                    .addExplicit("Approved (off-label)", "highestPhase")
                    .build();
            if (jsn.at("/label").isMissingNode()) {
                return Optional.empty();
            }
            return Optional.of(jsn);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    private Optional<JsonNode> extractUseGuide(JsonNode node) {


        JsonNode jsn = new JsonNodeMaker(node)
                .add("/InVivoUseGuide/0/value", "inVivoUse")
                .add("/InVivoUseRoute/0/value", "inVivoRoute")
                .add("/InVivoUseRouteOther/0/value", "inVivoRoute")
                .add("/InVivoComment/0/value", "inVivoComment")
                .addList("/InVivoUri/0/value", "inVivoUri", "\\|")

                .add("/InVitroUseGuide/0/value", "inVitroUse")
                .add("/InVitroComment/0/value", "inVitroRoute")
                .addList("/InVitroUri/0/value", "inVitroUri", "\\|")

                .build();


        if (jsn.at("/inVitroUse").isMissingNode() && jsn.at("/inVivoUse").isMissingNode()) {
            return Optional.empty();
        }
        return Optional.of(jsn);
    }

    private Optional<JsonNode> extractConditions(String base64Encoded) {
        try {
            byte[] decode = Base64.getDecoder().decode(base64Encoded);
            JsonNode original = new ObjectMapper().readTree(decode);
            JsonNode jsn = new JsonNodeMaker(original)
                    .add("id", "StitcherId")
                    .add("ConditionName", "label")
                    .add("ConditionMeshValue", "label")
                    .add("ConditionMeshValue", "meshTerm")
                    .addList("ConditionUri", "uri", "\\|")
                    .add("HighestPhase", "highestPhase")
                    .add("HighestPhaseUri", "highestPhaseUri")
                    .add("TreatmentModality", "modality")
                    .add("PrimaryTargetLabel", "targets")


                    //.add("ConditionManualProductName", "productName")
                    .add("ConditionProductName", "productName")

                    .add("ConditionFdaUse", "productFDAUse")
                    .add("FdaUseURI", "productFDAUseUri")
                    .add("FdaUseComment", "fdaUseComment")
                    .add("ConditionProductDate", "productDate")
                    .build();

            if (jsn.at("/label").isMissingNode()) {
                return Optional.empty();
            }

            // TODO check ohase nomenclature is correct
            String hphase = jsn.at("/highestPhase").textValue();

            List<String> CLINICAL_PHASES = Arrays.asList(
                    "Phase I", "Phase II", "Phase III", "Phase IV");

            Set<String> SORTED_PHASES;
            SORTED_PHASES = new LinkedHashSet<>(CLINICAL_PHASES);
            SORTED_PHASES.add("Approved (off-label)");
            SORTED_PHASES.add("Approved");

            if ("Approved".equals(hphase)) {
                ObjectNode on = (ObjectNode) jsn;
                on.put("isHighestPhaseApproved", "true");
                on.put("approvalSource", "RANCHO");
            } else {
                if (CLINICAL_PHASES.contains(hphase)) {
                    ObjectNode on = (ObjectNode) jsn;
                    on.put("isClinical", true);
                }
            }


            ((ObjectNode) jsn).set("TargetRefs", original.at("/TargetRefs"));

            return Optional.of(jsn);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private JsonNode extractTarget(String base64Encoded) {
        try {
            byte[] decode = Base64.getDecoder().decode(base64Encoded);

           /*
            js.at("/potencyValue").textValue() + " " +
	    				 js.at("/potencyDimensions").textValue() + " [" +
	    				 js.at("/potencyType").textValue() + "]";
            */
            JsonNode original = new ObjectMapper().readTree(decode);
            JsonNode node = new JsonNodeMaker(original)
                    .add("id", "StitcherId")
                    .add("PrimaryTargetId", "id")
                    .add("PrimaryTargetGeneId", "geneid")
                    .add("PrimaryTargetGeneSymbol", "geneSymbol")
                    .add("PrimaryTargetOrganism", "organism")


                    .add("PrimaryTargetLabel", "label")
                    .add("PrimaryTargetType", "type")
                    //TODO how are multiples listed in trung's json ?
                    .moveToArray("/PrimaryTargetUri", "/uri")

                    .add("TargetPharmacology", "pharmacology")
                    .add("PrimaryPotencyType", "potencyType")
                    .add("PrimaryPotencyValue", "potencyValue")
                    .add("PrimaryPotencyDimensions", "potencyDimensions")
                    .add("PrimaryPotencyUri", "potencyUri")
                    .add("ConditionMeshValue", "conditions")
                    .add("ConditionName", "conditions")

//                   .remove("ConditionRefs")
                    // .remove("id") // remove the substance id
                    .build();
            if (node.at("/label").isMissingNode()) {
                ((ObjectNode) node).set("label", node.at("/id"));
            }
            ((ObjectNode) node).set("ConditionRefs", original.at("/ConditionRefs"));

            return node;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Optional<JsonNode> extractDescription(JsonNode jsn) {

        return extractDescription(jsn, "CompoundDescription", "description");
    }

    private Optional<JsonNode> extractOriginator(JsonNode jsn) {

        return extractDescription(jsn, "Originator", "originator");
    }

    private Optional<JsonNode> extractCns(JsonNode jsn) {

        return extractDescription(jsn, "Cns", "cns");
    }

    private Optional<JsonNode> extractDescription(JsonNode jsn, String propertyPrefix, String ourName) {

        ObjectNode on = objectmapper.createObjectNode();
        //everything from stitcher is an array even if there's only 1 element
        forEachArrayValue(jsn, "/sgroup/properties/" + propertyPrefix, n -> on.replace(ourName, n.at("/value")));
        forEachArrayValue(jsn, "/sgroup/properties/" + propertyPrefix + "Comment", n -> on.replace("comment", n.at("/value")));

        //this is actually an array
        ArrayNode array = objectmapper.createArrayNode();
        forEachArrayValue(jsn, "/sgroup/properties/" + propertyPrefix + "Uri", n -> array.add(n.at("/value")));

        if (on.size() > 0 || array.size() > 0) {
            on.replace("uri", array);
            return Optional.of(on);
        }
        return Optional.empty();
    }


    private void forEachArrayValue(JsonNode node, String path, Consumer<JsonNode> consumer) {
        JsonNode pmids = node.at(path);

        if (pmids != null && !pmids.isMissingNode() && !pmids.isNull()) {
            ArrayNode array = (ArrayNode) pmids;
            array.forEach(n -> consumer.accept(n));
        }
    }

    // Perform Summary Calculation
    @Override
    public void accept(Stitch stitch) {
        Node _node = stitch._node();
        List<Event> events = new ArrayList<>();
        for (Relationship rel : _node.getRelationships(Direction.BOTH)) {
            Node n = rel.getOtherNode(_node);
            if (rel.isType(AuxRelType.EVENT)) {
                ObjectNode on = Util.toJsonNode(rel);
                Util.toJsonNode(on, n);
                events.add(new Event(on));
            }
        }

        logger.info("Stitch "+stitch.getId()+" => "+ stitch._properties().toString() + " => " + events.size() +
                " event events");

        Calendar cal = Calendar.getInstance();

        // Calculate Initial Approval
        Event initUSAppr = null;
        Event initAppr = null;
        for (Event e: events) {
            if (e.startDate != null && "US".equals(e.jurisdiction) && e.kind.isApproved()) {
                if (initAppr == null || e.startDate.before(initAppr.startDate)) {
                    initAppr = e;
                }
                if (initUSAppr == null || e.startDate.before(initUSAppr.startDate)) {
                    initUSAppr = e;
                }
            }
            if (e.startDate != null && (e.kind.isApproved() || e.kind == Event.EventKind.Marketed)) {
                if (initAppr == null || e.startDate.before(initAppr.startDate)) {
                    initAppr = e;
                }
            }
        }

        if (initAppr != null) {
            cal.setTime(initAppr.startDate);
            System.out.println("Initially marketed: "+cal.get(Calendar.YEAR)+ " ("+initAppr.jurisdiction+")");
        }
        if (initUSAppr != null && initAppr != initUSAppr) {
            cal.setTime(initUSAppr.startDate);
            System.out.println("Initial US marketing: "+cal.get(Calendar.YEAR));
        }

        // Calculate Current Marketing Status
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+SummaryEventCalculator.class.getName()
                    +" Node_ID");
            System.exit(1);
        }

        EntityFactory ef = new EntityFactory (GraphDb.getInstance(argv[0]));
        SummaryEventCalculator ac = new SummaryEventCalculator(ef);
        //int version = Integer.parseInt(argv[1]);
        //int count = ac.recalculate(version);
        long[] nodes = new long[argv.length-1];
        for (int i=1; i<argv.length; i++)
            nodes[i-1] = Long.parseLong(argv[i]);
        int count = ac.recalculateNodes(nodes);
        logger.info(count+" stitches recalculated!");
        ef.shutdown();
    }
}