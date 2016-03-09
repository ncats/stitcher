package services;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import javax.inject.Inject;
import javax.inject.Singleton;

import play.cache.CacheApi;
import play.Logger;
import play.Application;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.Statistics;

@Singleton
public class CacheService implements CacheApi {
    static final int MAX_ELEMENTS = 10000;
    static final int TIME_TO_LIVE = 2*60*60; // 2hr
    static final int TIME_TO_IDLE = 2*60*60; // 2hr

    public static final String CACHE_MAX_ELEMENTS = "ix.cache.maxElements";
    public static final String CACHE_TIME_TO_LIVE = "ix.cache.timeToLive";
    public static final String CACHE_TIME_TO_IDLE = "ix.cache.timeToIdle";

    final Ehcache cache;

    @Inject
    public CacheService (Application app, ApplicationLifecycle lifecycle) {
        int maxElements = app.configuration()
            .getInt(CACHE_MAX_ELEMENTS, MAX_ELEMENTS);
        CacheConfiguration config =
            new CacheConfiguration (getClass().getName(), maxElements)
            .timeToLiveSeconds(app.configuration()
                               .getInt(CACHE_TIME_TO_LIVE, TIME_TO_LIVE))
            .timeToIdleSeconds(app.configuration()
                               .getInt(CACHE_TIME_TO_IDLE, TIME_TO_IDLE));
        cache = CacheManager.getInstance()
            .addCacheIfAbsent(new Cache (config));
        cache.setSampledStatisticsEnabled(true);

        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);
            });
    }

    public void shutdown () {
        try {
            cache.dispose();
            CacheManager.getInstance().removeCache(cache.getName());
        }
        catch (Exception ex) {
            Logger.trace("Disposing cache", ex);
        }
    }

    public Element getElement (String key) {
        return cache.get(key);
    }
    
    public Object get (String key) {
        Element elm = cache.get(key);
        return elm != null ? elm.getObjectValue() : null;
    }

    public long getLastAccessTime (String key) {
        Element elm = cache.get(key);
        return elm != null ? elm.getLastAccessTime() : 0l;
    }

    public long getExpirationTime (String key) {
        Element elm = cache.get(key);
        return elm != null ? elm.getExpirationTime() : 0l;
    }

    public boolean isExpired (String key) {
        Element elm = cache.get(key);
        return elm != null ? elm.isExpired() : false;
    }

    /**
     * apply generator if the cache was created before epoch
     */
    public <T> T getOrElse (long epoch, String key, Callable<T> generator)
        throws Exception {
        Element elm = cache.get(key);
        if (elm == null || elm.getCreationTime() < epoch) {
            T v = generator.call();
            elm = new Element (key, v, true /* eternal*/, null, null);
            cache.put(elm);
        }
        return (T)elm.getObjectValue();
    }
    
    public <T> T getOrElse (String key, Callable<T> generator)  {
        
        Object value = get (key);
        if (value == null) {
            try {
                T v = generator.call();
                cache.put(new Element (key, v));
                return v;
            }
            catch (Exception ex) {
                Logger.trace("getOrElse", ex);
            }
        }
        return (T)value;
    }

    // mimic play.Cache 
    public <T> T getOrElse (String key, Callable<T> generator, int seconds) {
        Object value = get (key);
        if (value == null) {
            try {
                T v = generator.call();
                cache.put(new Element (key, v, seconds <= 0, seconds, seconds));
                return v;
            }
            catch (Exception ex) {
                Logger.trace("getOrElse", ex);
            }
        }
        return (T)value;
    }

    public List getKeys () {
        try {
            return new ArrayList (cache.getKeys());
        }
        catch (Exception ex) {
            Logger.trace("Can't get cache keys", ex);
        }
        return null;
    }
    
    public List getKeys (int top, int skip) {
        List keys = getKeys ();
        if (keys != null) {
            keys = keys.subList(skip, Math.min(skip+top, keys.size()));
        }
        return keys;
    }
    
    public void set (String key, Object value) {
        cache.put(new Element (key, value));
    }

    public void set (String key, Object value, int expiration) {
        cache.put(new Element (key, value,
                               expiration <= 0, expiration, expiration));
    }

    public void remove (String key) {
        try {
            cache.remove(key);
        }
        catch (Exception ex) {
            Logger.warn("Removing key "+key, ex);
        }
    }
    
    public Statistics getStatistics () {
        return cache.getStatistics();
    }

    public boolean contains (String key) {
        return cache.isKeyInCache(key);
    }
}
