package services;

import akka.actor.*;
import play.Logger;

public class WebSocketEchoActor extends UntypedActor {
    public static Props props (ActorRef ref) {
        return Props.create(WebSocketEchoActor.class, ref);
    }

    private final ActorRef out;
    public WebSocketEchoActor (ActorRef out) {
        this.out = out;
        Logger.debug("WebSocketConsoleActor created for "+out);
    }

    public void onReceive (Object message) throws Exception {
        out.tell(new java.util.Date()+": "+message, self ());
    }
}
