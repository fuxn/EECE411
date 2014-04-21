package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;

import Exception.InexistentKeyException;
import Exception.OutOfSpaceException;
import Interface.CommandHandler;
import KVStore.ConsistentHash;
import NIO.Dispatcher;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class PutReplicaCommandHandler implements CommandHandler {

	@Override
	public void executCommand(Selector selector, SelectionKey handle,
			byte[] key, byte[] value) {
		System.out.println("***************PUT REPLICA******************");

		Integer keyHash = Arrays.hashCode(key);
		byte[] reply = null;
		try {
			reply = ConsistentHash.local.put(keyHash, value);

		} catch (InexistentKeyException e) {
			e.printStackTrace();
			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());
		} catch (OutOfSpaceException e) {
			reply = MessageUtilities.formateReplyMessage(ErrorEnum.OUT_OF_SPACE
					.getCode());
		} catch (Exception e) {
			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		}
		Dispatcher.response(handle, reply);
	}

}
