package services;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Singleton;

import play.inject.ApplicationLifecycle;
import play.Application;
import play.Logger;
import play.Configuration;
import play.libs.F;
import play.db.ebean.Transactional;

import java.security.MessageDigest;
import java.security.SecureRandom;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import models.Principal;

@Singleton
public class UserService {
    static final String ADMIN_USER = "admin";

    final Principal admin; // administrator
    final Application app;

    @Inject
    public UserService (Application app, ApplicationLifecycle lifecycle) {
        this.app = app;

        List<Principal> results = Principal.find
            .where().eq("username", ADMIN_USER).findList();
        if (results.isEmpty()) {
            admin = new Principal (ADMIN_USER);
            admin.save();
        }
        else {
            admin = results.iterator().next();
        }

        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);            
            });
    }

    protected void shutdown () {
        Logger.debug("Shutting down "+getClass().getName()+"..."+this);
    }

    /*
     * get current principal
     */
    public Principal getPrincipal () {
        return admin;
    }
}
