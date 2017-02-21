package ncats.stitcher;

/**
 * curation status
 */
public enum Status {
    ManualCuration, // manually asserted/curated
    SourceAssertion, // source asserts is true
    ComputedAssertion, // computed based on source info, treat with caution
    Ambiguous, // fact true in context, but not globally unique/true
    Suspicious, // supposedly fact is true, but if so violates conventional wisdom
    Deprecated, // past assertion is no longer asserted
    Invalid, // rejected/invalid
    Unknown
}
