package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import Interface.CommandHandler;
import KVStore.ConsistentHash;

public class GetCommandHandler implements CommandHandler {

	@Override
	public void executCommand(ConsistentHash cHash, Selector selector,
			SelectionKey handle, byte[] key, byte[] value) {
		cHash.get(selector, handle, key, value);
	}

}
