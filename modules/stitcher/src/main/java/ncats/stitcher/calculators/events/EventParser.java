package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.List;
import java.util.Map;

public abstract class EventParser {
    final public String name;
    protected EventParser(String name) {
        this.name = name;
    }

    public abstract List<Event> getEvents (Map<String, Object> payload);
}
