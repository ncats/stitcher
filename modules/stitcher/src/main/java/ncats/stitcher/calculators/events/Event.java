package ncats.stitcher.calculators.events;

import com.fasterxml.jackson.databind.JsonNode;
import ncats.stitcher.calculators.EventCalculator;

import java.lang.reflect.Field;
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
        Marketed {
            @Override
            public boolean wasMarketed(){
                return true;
            }
        },
        ApprovalOTC {
            @Override
            public boolean isApproved(){
                return true;
            }
        },
        Withdrawn {
            @Override
            public boolean wasMarketed(){
                return true;
            }
        },
        Other
        ;

        public boolean isApproved(){
            return false;
        }

        public boolean wasMarketed(){
            return isApproved();
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

    public Event(JsonNode node) {
        for (Field field: this.getClass().getDeclaredFields()) {
            if (node.has(field.getName())) {
                try {
                    if (field.getType() == String.class || field.getType() == Object.class)
                        field.set(this, node.get(field.getName()).textValue());
                    else if (field.getType() == Date.class) {
                        String str = node.get(field.getName()).textValue();
                        int year = Integer.valueOf(str.substring(0, 4));
                        int month = Integer.valueOf(str.substring(str.indexOf("-")+1,str.lastIndexOf("-")));
                        int day = Integer.valueOf(str.substring(str.lastIndexOf("-")+1));
                        Date date = new Date(year-1900, month-1, day);
                        field.set(this, date);
                    }
                    else if (field.getType() == EventKind.class) {
                        String str = node.get(field.getName()).textValue();
                        EventKind kind = EventKind.valueOf(str);
                        field.set(this, kind);
                    }
                    else throw new IllegalArgumentException();
                } catch (Exception e) {
                    System.err.println("Could not set:" + field.getName() + " to value " + node.get(field.getName()));
                }
            }
        }
    }
}
