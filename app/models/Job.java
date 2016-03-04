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
    @Version public Long version;
    @CreatedTimestamp Timestamp created;
    @UpdatedTimestamp Timestamp updated;

    @OneToOne(cascade=CascadeType.ALL)
    public Principal owner;
    
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

    public Job setKey (String key) {
        this.key = key;
        return this;
    }
    public String getKey () { return key; }
    
    public Job setStatus (Status status) {
        this.status = status;
        return this;
    }
    public Status getStatus () { return status; }

    public Job setOwner (Principal owner) {
        this.owner = owner;
        return this;
    }
    public Principal getOwner () { return owner; }

    public Job setFinished (Long finished) {
        this.finished = finished;
        return this;
    }
    public Long getFinished () { return finished; }

    public Job setStarted (Long started) {
        this.started = started;
        return this;
    }
    public Long getStarted () { return started; }

    public Job setPayload (Payload payload) {
        this.payload = payload;
        return this;
    }
    public Payload getPayload () { return payload; }
}
