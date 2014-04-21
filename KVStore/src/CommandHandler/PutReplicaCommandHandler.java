package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import Interface.CommandHandler;
import KVStore.ConsistentHash;

public class PutReplicaCommandHandler implements CommandHandler {

	@Override
	public void executCommand(ConsistentHash cHash, Selector selector,
			SelectionKey handle, byte[] key, byte[] value) {
		cHash.putReplica(selector, handle, key, value);
	}

}
