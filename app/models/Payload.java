package models;

import java.util.*;
import java.sql.Timestamp;
import javax.persistence.*;

import com.avaje.ebean.Model;
import com.avaje.ebean.annotation.CreatedTimestamp;
import com.avaje.ebean.annotation.UpdatedTimestamp;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
public class Payload extends Model {
    public static Find<Long, Payload> find =
        new Finder<Long, Payload>(Payload.class);

    public static Payload getInstance (Long id) {
        return find.byId(id);
    }

    @JsonIgnore
    @Id public Long id;
    @Version public Long version;
    @CreatedTimestamp
    public Timestamp created;
    @UpdatedTimestamp
    public Timestamp updated;

    @JsonIgnore
    private Boolean deleted;

    @OneToOne(cascade=CascadeType.ALL)
    public Principal owner;

    @JsonIgnore
    @Column(length=40,nullable=false)
    public String uuid;
    
    @Column(length=40,nullable=false)
    public String sha1;
    
    @Column(length=255)
    public String title;
    public boolean shared;
    @Column(length=255)
    public String filename;
    public Long size; // file size
    public Integer count; // number of records

    @Column(length=128)
    public String mimeType;
    public String format; // parsing format.. not same as storage format

    @Lob
    public String uri; // source uri
    public String key; // ix.curation.DataSource key
    
    @Lob
    public String comments;

    @OneToMany(cascade=CascadeType.ALL)
    public List<Property> properties = new ArrayList<Property>();

    public Payload () {
    }

    public Payload setUuid (String uuid) {
        this.uuid = uuid;
        return this;
    }
    public String getUuid () { return uuid; }

    public Payload setSha1 (String sha1) {
        this.sha1 = sha1;
        return this;
    }
    public String getSha1 () { return sha1; }

    public Payload setOwner (Principal owner) {
        this.owner = owner;
        return this;
    }
    public Principal getOwner () { return owner; }
    
    public Payload setTitle (String title) {
        this.title = title;
        return this;
    }
    public String getTitle () { return title; }

    public Payload setShared (boolean shared) {
        this.shared = shared;
        return this;
    }
    public boolean getShared () { return shared; }

    public Payload setFilename (String filename) {
        this.filename = filename;
        return this;
    }
    public String getFilename () { return filename; }

    public Payload setSize (Long size) {
        this.size = size;
        return this;
    }
    public Long getSize () { return size; }

    public Payload setCount (Integer count) {
        this.count = count;
        return this;
    }
    public Integer getCount () { return count; }

    public Payload setMimeType (String mimeType) {
        this.mimeType = mimeType;
        return this;
    }
    public String getMimeType () { return mimeType; }

    public Payload setUri (String uri) {
        this.uri = uri;
        return this;
    }
    public String getUri () { return uri; }

    public Payload setKey (String key) {
        this.key = key;
        return this;
    }
    public String getKey () { return key; }

    public Payload setComments (String comments) {
        this.comments = comments;
        return this;
    }
    public String getComments () { return comments; }

    public Payload setProperties (List<Property> properties) {
        this.properties = properties;
        return this;
    }
    public List<Property> getProperties () { return properties; }

    public Payload setDeleted (boolean deleted) {
        this.deleted = deleted;
        return this;
    }
    public Boolean getDeleted () { return deleted; }
    
    @JsonIgnore
    public String sha1 () { return sha1.substring(0, 9); }
}
