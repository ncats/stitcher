package services;

import akka.actor.*;
import play.Logger;

public class WebSocketConsoleActor extends UntypedActor {
    final ActorRef out;
    final String key;
    final CacheService cache;

    public static class Message {
        final String text;
        public Message (String text) {
            this.text = text;
        }
        public String toString () {
            return text;
        }
    }

    public static Message message (String text) {
        return new Message (text);
    }

    public WebSocketConsoleActor
        (ActorRef out, String key, CacheService cache) {
        this.out = out;
        this.key = key;
        this.cache = cache;
        cache.set(key, self ());
        Logger.debug("WebSocketConsoleActor created "+self().path()
                     +" for key="+key+" "+out);
    }

    public void onReceive (Object message) throws Exception {
        if (message instanceof Message) {
            out.tell(new java.util.Date()+": "+message, self ());
        }
        else {
            unhandled (message);
        }
    }

    @Override
    public void postStop () {
        Logger.debug("Closing console actor "+out);
        try {
            cache.remove(key);
        }
        catch (Exception ex) {
            Logger.warn("Removing key "+key, ex);
        }
    }
}
