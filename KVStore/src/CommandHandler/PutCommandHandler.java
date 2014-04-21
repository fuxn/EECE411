package CommandHandler;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.List;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.OutOfSpaceException;
import Interface.CommandHandler;
import KVStore.ConsistentHash;
import KVStore.KVStore;
import NIO.Dispatcher;
import Utilities.ChordTopologyService;
import Utilities.CommandEnum;
import Utilities.ConnectionService;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class PutCommandHandler implements CommandHandler {

	@Override
	public void executCommand(Selector selector, SelectionKey handle,
			byte[] key, byte[] value) {
		System.out.println("***************PUT******************");
		Integer keyHash = Arrays.hashCode(key);
		byte[] reply = null;

		try {
			List<String> coord = ChordTopologyService
					.getCoordinatorAndReplicas(keyHash);
			ByteBuffer message = MessageUtilities.requestMessage(
					CommandEnum.PUT_REPLICA.getCode(), key, value);

			if (coord.contains(KVStore.localHost)) {

				reply = ConsistentHash.local.put(keyHash, value);
				coord.remove(KVStore.localHost);
				ConsistentHash.putToReplica(coord, handle, message, keyHash);
			} else {
				ConnectionService.connectToNIORemote(
						coord.get(0),
						handle,
						MessageUtilities.requestMessage(
								CommandEnum.PUT.getCode(), key, value));
				return;
			}

		} catch (InexistentKeyException e) {
			System.out.println("inexistent");
			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());
		} catch (InternalKVStoreFailureException e) {
			System.out.println("internal");

			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		} catch (OutOfSpaceException e) {
			System.out.println("outofspace");

			reply = MessageUtilities.formateReplyMessage(ErrorEnum.OUT_OF_SPACE
					.getCode());
		} catch (Exception e) {
			System.out.println("exception");
			e.printStackTrace();

			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		}

		Dispatcher.response(handle, reply);
	}

}
