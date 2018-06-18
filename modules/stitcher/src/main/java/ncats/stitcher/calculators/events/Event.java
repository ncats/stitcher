package ncats.stitcher.calculators.events;

import ncats.stitcher.calculators.EventCalculator;

import java.util.Date;

public class Event implements Cloneable{
    public EventKind kind;
    public String source;
    public Object id;
    public Date date;
    public String jurisdiction;
    public String comment; // reference

    public String route;

    public String approvalAppId;

    public String marketingStatus;
    public String productCategory;

    public String NDC;
    public String URL;

    public Integer withDrawnYear;

    /**
     * Create a deep clone.
     * @return returns an event.
     */
    @Override
    public Event clone()  {
        Event e = null;
        try {
            e = (Event) super.clone();
        } catch (CloneNotSupportedException e1) {
            throw new RuntimeException(e1); //shouldn't happen
        }
        //date is mutable so make defensive copy
        if(date != null) {
            e.date = new Date(date.getTime());
        }
        return e;
    }

    public enum EventKind {
        Publication,
        Filing,
        Designation,
        ApprovalRx {
            @Override
            public boolean isApproved() {
                return true;
            }
        },
        Marketed,
        ApprovalOTC {
            @Override
            public boolean isApproved(){
                return true;
            }
        },
        Other,
        Withdrawn
        ;

        public boolean isApproved(){
            return false;
        }
    }

    public Event(String source, Object id, EventKind kind, Date date) {
        this.source = source;
        this.id = id != null ? id : "*";
        this.kind = kind;
        this.date = date;
    }

    public Event(String source, Object id, EventKind kind) {
        this(source, id, kind, null);
    }
}
