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

public class GetReplicaCommandHandler implements CommandHandler {

	@Override
	public void executCommand(Selector selector, SelectionKey handle,
			byte[] key, byte[] value) {
		System.out.println("***************GET REPLICA******************");

		Integer keyHash = Arrays.hashCode(key);
		byte[] replyMessage = null;
		try {
			replyMessage = ConsistentHash.local.getReplica(keyHash);

		} catch (InexistentKeyException e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());
		} catch (Exception e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		}

		Dispatcher.response(handle, replyMessage);

	}

}
