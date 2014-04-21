package Interface;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;


public interface CommandHandler {

	public void executCommand(Selector selector, SelectionKey handle,
			byte[] key, byte[] value);
}
