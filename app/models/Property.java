package models;

import java.util.*;
import java.sql.Timestamp;
import javax.persistence.*;

import io.ebean.Model;
import io.ebean.Finder;
import io.ebean.annotation.CreatedTimestamp;
import io.ebean.annotation.UpdatedTimestamp;

@Entity
public class Property extends Model {
    public static Finder<Long, Property> find = new Finder<>(Property.class);
    
    @Id public Long id;
    @Version public Long version;
    @CreatedTimestamp public Timestamp created;
    @UpdatedTimestamp public Timestamp updated;

    @Column(nullable=false)
    public String name;
    public String type;
    public Integer count;

    public Property () {
    }
    public Property (String name) {
        this.name = name;
    }

    public Property setName (String name) {
        this.name = name;
        return this;
    }
    public String getName () { return name; }

    public Property setType (String type) {
        this.type = type;
        return this;
    }
    public String getType () { return type; }

    public Property setCount (Integer count) {
        this.count = count;
        return this;
    }
    public Integer getCount () { return count; }
}
