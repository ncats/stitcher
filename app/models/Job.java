package models;

import java.util.*;
import java.sql.Timestamp;
import javax.persistence.*;

import com.avaje.ebean.Model;
import com.avaje.ebean.annotation.CreatedTimestamp;
import com.avaje.ebean.annotation.UpdatedTimestamp;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
public class Job extends Model {
    public static Find<Long, Job> find = new Finder<Long, Job>(Job.class);

    public enum Status {
        FIRED,
        FAILED,
        DONE
    }
    
    @Id public Long id;
    @Version Long version;
    @CreatedTimestamp Timestamp created;
    @UpdatedTimestamp Timestamp updated;
    
    @Column(nullable=false)
    public String key; // job key
    public Status status;
    public String name;

    public Long started;
    public Long finished;

    @ManyToOne(cascade=CascadeType.ALL)
    public Payload payload;

    public Job () {
    }

    public void setStatus (Status status) {
        this.status = status;
    }

    public void setFinished (Long finished) {
        this.finished = finished;
    }

    public void setPayload (Payload payload) {
        this.payload = payload;
    }
}
