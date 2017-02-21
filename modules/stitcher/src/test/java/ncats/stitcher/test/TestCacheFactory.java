package ncats.stitcher.test;

import java.util.*;
import java.util.concurrent.Callable;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;

import ncats.stitcher.CacheFactory;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

public class TestCacheFactory {
    static final Logger logger =
        Logger.getLogger(TestCacheFactory.class.getName());

    static {
        CacheFactory.addShutdownHook();
    }
    
    @Rule public TestName name = new TestName();
    
    public TestCacheFactory () {
    }

    @Test
    public void test1 () throws Exception {
        File dir = TestUtil.createTempDir(name.getMethodName());
        CacheFactory cache = CacheFactory.getInstance(dir);
        cache.put("key1000", "value1000");
        assertTrue ("value1000".equals(cache.get("key1000")));
        cache.remove("key1000");
        assertTrue (null == cache.get("key1000"));
        
        for (int i = 0; i < 1000; ++i) {
            cache.put("key"+i, "value"+i);
        }
        cache.shutdown();
        
        cache = CacheFactory.getInstance(dir);
        for (int i = 0; i < 1000; ++i) {
            Object value = cache.get("key"+i);
            assertTrue ("There should be key"+i+" => value"+i,
                        value.equals("value"+i));
        }
        cache.shutdown();
    }
}
