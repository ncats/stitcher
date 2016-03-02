package models;

import java.util.*;
import javax.persistence.*;

import com.avaje.ebean.Model;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
public class Job extends Model {
    public static Find<Long, Job> find = new Finder<Long, Job>(Job.class);

    public enum Status {
        WAITING,
        FAILED,
        DONE
    }
    
    @Id public Long id;
    public String subId; // submission id
    public Status status = Status.WAITING;
    public String name;

    public Long started; // time job started
    public Long finished; // time job finished

    @ManyToOne(cascade=CascadeType.ALL)
    public Payload payload;

    public Job () {
    }
}
