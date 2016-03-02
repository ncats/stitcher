package models;

import java.util.*;
import javax.persistence.*;

import com.avaje.ebean.Model;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
public class Payload extends Model {
    public static Find<Long, Payload> find =
        new Finder<Long, Payload>(Payload.class);
    
    @Id
    public Long id;

    @Column(length=128)
    public String uuid;
    public Long timestamp;
    
    @Column(length=40,nullable=false)
    public String sha1;
    
    @Column(length=255)
    public String title;
    public boolean shared;
    @Column(length=255)
    public String filename;
    public Long size;

    @Column(length=128)
    public String mimeType;
    public String format;

    @Lob
    public String uri;
    
    @Lob
    public String comments;

    public Payload () {
    }

    @JsonIgnore
    public String sha1 () { return sha1.substring(0,7); }
}
