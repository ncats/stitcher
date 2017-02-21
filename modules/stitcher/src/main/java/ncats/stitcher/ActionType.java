package ncats.stitcher;

import java.util.HashMap;
import java.util.Map;

/**
 * These are the edge properties derived from ChEMBL_20 action_type
 */
public enum ActionType  {
    Interacts("INTERACTS"), // a generic action type that subsumes all other values here
    Inhibitor("INHIBITOR"),
    Agonist("AGONIST"),
    Antagonist("ANTAGONIST"),
    Blocker("BLOCKER"),
    Positive_Allosteric_Modulator("POSITIVE ALLOSTERIC MODULATOR"),
    Opener("OPENER"),
    Hydrolytic_Enzyme("HYDROLYTIC ENZYME"),
    Chelating_Agent("CHELATING AGENT"),
    Releasing_Agent("RELEASING AGENT"),
    Activator("ACTIVATOR"),
    Modulator("MODULATOR"),
    Positive_Modulator("POSITIVE MODULATOR"),
    Partial_Agonist("PARTIAL AGONIST"),
    Negative_Allosteric_Modulator("NEGATIVE ALLOSTERIC MODULATOR"),
    Substrate("SUBSTRATE"),
    Binding_Agent("BINDING AGENT"),
    Inverse_Agonist("INVERSE AGONIST"),
    Proteolytic_Enzyme("PROTEOLYTIC ENZYME"),
    Oxidative_Enzyme("OXIDATIVE ENZYME"),
    Antisense_Inhibitor("ANTISENSE INHIBITOR"),
    Cross_Linking_Agent("CROSS LINKING ENZYME"),
    Allosteric_Antagonist("ALLOSTERIC ANTAGONIST");

    private final String textValue;
    private static final Map<String, ActionType> lookup = new HashMap<>();

    static {
        for (ActionType at : ActionType.values()) lookup.put(at.getTextValue(), at);
    }

    public String getTextValue() {
        return textValue;
    }

    public static ActionType get(String textValue) {
        if (textValue == null) return (null);
        return lookup.get(textValue);
    }

    private ActionType(String textValue) {
        this.textValue = textValue;
    }
}
