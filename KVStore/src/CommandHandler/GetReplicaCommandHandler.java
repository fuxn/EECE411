package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import Interface.CommandHandler;
import KVStore.ConsistentHash;

public class GetReplicaCommandHandler implements CommandHandler{

	@Override
	public void executCommand(ConsistentHash cHash, Selector selector,
			SelectionKey handle, byte[] key, byte[] value) {
		cHash.getReplica(selector, handle, key, value);
		
	}

}
