package services;

import java.io.*;
import java.util.concurrent.Callable;

import play.cache.CacheApi;
import play.Logger;

import ix.curation.CacheFactory;

public class CacheService implements CacheApi {
    CacheFactory cache;

    protected CacheService (CacheFactory cache) {
        this.cache = cache;
    }

    public <T> T get (String key) {
        return (T)cache.get(key);
    }
    public <T> T getOrElse (String key, Callable<T> call) {
        return cache.getOrElse(key, call);
    }
    public <T> T getOrElse (String key, Callable<T> call, int expiration) {
        return cache.getOrElse(key, call);
    }
    public void remove (String key) {
        cache.remove(key);
    }
    public void set (String key, Object value) {
        cache.put(key, value);
    }
    public void set (String key, Object value, int expiration) {
        cache.put(key, value);
    }
}
