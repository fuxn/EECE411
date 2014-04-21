package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import Interface.CommandHandler;
import KVStore.ConsistentHash;

public class DeleteReplicaCommandHandler implements CommandHandler{

	@Override
	public void executCommand(ConsistentHash cHash, Selector selector,
			SelectionKey handle, byte[] key, byte[] value) {
		cHash.removeReplica(selector, handle, key, value);
		
	}

	
}
