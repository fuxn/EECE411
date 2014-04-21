package Interface;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import KVStore.ConsistentHash;

public interface CommandHandler {

	public void executCommand(ConsistentHash cHash, Selector selector, SelectionKey handle,
			byte[] key, byte[] value);
}
