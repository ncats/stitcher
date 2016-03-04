package models;

import java.util.UUID;
import java.sql.Timestamp;
import javax.persistence.*;

import com.avaje.ebean.Model;
import com.avaje.ebean.annotation.CreatedTimestamp;
import com.avaje.ebean.annotation.UpdatedTimestamp;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
public class Principal extends Model {
    public static Find<Long, Principal> find =
        new Finder<Long, Principal>(Principal.class);

    @Id public UUID id;
    @Version Long version;
    @CreatedTimestamp public Timestamp created;
    @UpdatedTimestamp public Timestamp updated;

    @Column(nullable=false,unique=true)
    public String username;
    public String email;
    
    @Lob
    public String provider;
    public boolean blocked = true; // user current bein blocked

    @Lob
    public byte[] avatar;

    public Principal () {}
    public Principal (String username) {
        this.username = username;
    }

    public Principal setUsername (String username) {
        this.username = username;
        return this;
    }
    public String getUsername () { return username; }
    
    public Principal setProvider (String provider) {
        this.provider = provider;
        return this;
    }
    public String getProvider () { return provider; }
    
    public Principal setAvatar (byte[] avatar) {
        this.avatar = avatar;
        return this;
    }
    public byte[] getAvatar () { return avatar; }
    
    public Principal setEmail (String email) {
        this.email = email;
        return this;
    }
    public String getEmail () { return email; }

    public Principal setBlocked (boolean blocked) {
        this.blocked = blocked;
        return this;
    }
    public boolean getBlocked () { return blocked; }
}

