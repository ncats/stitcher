package models;

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

    @Id public Long id;
    @Version Long version;
    @CreatedTimestamp Timestamp created;
    @UpdatedTimestamp Timestamp updated;

    @Column(nullable=false)
    public String username;
    public String email;
    
    @Lob
    public String provider;

    @Lob
    public byte[] avatar;

    public Principal () {
    }

    public void setProvider (String provider) {
        this.provider = provider;
    }
    public void setAvatar (byte[] avatar) {
        this.avatar = avatar;
    }
    public void setUsername (String username) {
        this.username = username;
    }
    public void setEmail (String email) {
        this.email = email;
    }
}

