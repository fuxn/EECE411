package CommandHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import Interface.CommandHandler;
import KVStore.ConsistentHash;
import NIO.Dispatcher;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class AnnounceFailureCommandHandler implements CommandHandler{

	@Override
	public void executCommand(ConsistentHash cHash, Selector selector,
			SelectionKey handle, byte[] key, byte[] value) {
		Dispatcher.stopAccept();
		Dispatcher.response(handle, MessageUtilities
				.formateReplyMessage(ErrorEnum.SUCCESS.getCode()));

		System.exit(0);
		
	}

}
