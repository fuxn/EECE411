package CommandHandler;

import java.nio.ByteBuffer;
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

public class GetCommandHandler implements CommandHandler {

	@Override
	public void executCommand(Selector selector, SelectionKey handle,
			byte[] key, byte[] value) {
		Integer keyHash = Arrays.hashCode(key);
		System.out.println("***************GET******************");
		byte[] replyMessage = null;
		List<String> coords;
		try {
			coords = ChordTopologyService.getCoordinatorAndReplicas(keyHash);

			try {

				if (coords.contains(KVStore.localHost)) {
					System.out.println("get local ");
					replyMessage = ConsistentHash.local.get(keyHash);
				} else {

					System.out.println("get remote " + handle.isValid());
					ConnectionService.connectToNIORemote(
							coords.get(0),
							handle,
							MessageUtilities.requestMessage(
									CommandEnum.GET.getCode(), key, value));
					return;

				}
			} catch (InexistentKeyException e) {
				e.printStackTrace();
				ByteBuffer message = MessageUtilities.requestMessage(
						CommandEnum.GET_REPLICA.getCode(), key, value);
				coords.remove(KVStore.localHost);
				try {
					ConsistentHash.getFromReplica(coords, handle, message,
							keyHash);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				return;
			} catch (InternalKVStoreFailureException e) {
				e.printStackTrace();
				replyMessage = MessageUtilities
						.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE
								.getCode());
			} catch (Exception e) {
				e.printStackTrace();
				replyMessage = MessageUtilities
						.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE
								.getCode());
			}
		} catch (InexistentKeyException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InternalKVStoreFailureException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Dispatcher.response(handle, replyMessage);
	}

}
