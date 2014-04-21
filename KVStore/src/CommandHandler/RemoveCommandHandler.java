package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.List;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Interface.CommandHandler;
import KVStore.ConsistentHash;
import KVStore.KVStore;
import NIO.Dispatcher;
import Utilities.ChordTopologyService;
import Utilities.CommandEnum;
import Utilities.ConnectionService;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class RemoveCommandHandler implements CommandHandler {

	@Override
	public void executCommand(Selector selector, SelectionKey handle,
			byte[] key, byte[] value) {
		System.out.println("***************REMOVE******************");

		Integer keyHash = Arrays.hashCode(key);
		byte[] replyMessage = null;
		try {
			List<String> coords = ChordTopologyService
					.getCoordinatorAndReplicas(keyHash);
			if (coords.contains(KVStore.localHost)) {
				replyMessage = ConsistentHash.local.remove(keyHash);
				coords.remove(KVStore.localHost);
				ConsistentHash.removeFromReplica(coords, handle,
						MessageUtilities.requestMessage(
								CommandEnum.DELETE_REPLICA.getCode(), key,
								value), keyHash);
			} else {
				ConnectionService.connectToNIORemote(
						coords.get(0),
						handle,
						MessageUtilities.requestMessage(
								CommandEnum.DELETE.getCode(), key, value));
				return;
			}

		} catch (InexistentKeyException e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());
		} catch (InternalKVStoreFailureException e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		} catch (Exception e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		}

		Dispatcher.response(handle, replyMessage);

	}

}
