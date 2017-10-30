package models;

import io.ebean.config.ServerConfig;
import io.ebean.event.ServerConfigStartup;

import play.Logger;

public class DefaultServerConfigStartup implements ServerConfigStartup {
    public void onStart (ServerConfig serverConfig) {
        Logger.debug("** Configuring Ebean database... ");      
        serverConfig.setDatabaseSequenceBatchSize(5);
    }
}
