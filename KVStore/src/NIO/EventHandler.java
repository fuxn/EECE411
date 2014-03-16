package NIO;

import java.nio.channels.SelectionKey;

public interface EventHandler {

	public void handleEvent(SelectionKey handle) throws Exception;

}
