package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;

import Exception.InexistentKeyException;
import Interface.CommandHandler;
import KVStore.ConsistentHash;
import NIO.Dispatcher;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class DeleteReplicaCommandHandler implements CommandHandler {

	@Override
	public void executCommand(Selector selector, SelectionKey handle,
			byte[] key, byte[] value) {
		System.out.println("***************REMOVE REPLICA******************");

		Integer keyHash = Arrays.hashCode(key);
		byte[] reply = null;
		try {
			reply = ConsistentHash.local.remove(keyHash);

		} catch (InexistentKeyException e) {
			e.printStackTrace();
			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());
		} catch (Exception e) {
			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		}
		Dispatcher.response(handle, reply);

	}

}
