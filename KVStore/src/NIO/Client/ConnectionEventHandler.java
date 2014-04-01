package NIO.Client;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import KVStore.ConsistentHash;
import NIO.EventHandler;
import Utilities.CommandEnum;
import Utilities.Message.RemoteMessage;

public class ConnectionEventHandler implements EventHandler {
	private Selector selector;

	public ConnectionEventHandler(Selector demultiplexer) {
		this.selector = demultiplexer;
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel channel = (SocketChannel) handle.channel();
		RemoteMessage message = (RemoteMessage) handle.attachment();
		long endTimeMillis = System.currentTimeMillis() + 5000L;
		try {
			while ((!channel.finishConnect())
					&& (System.currentTimeMillis() < endTimeMillis)) {
				System.out.println("pending connection");
			}
			channel.configureBlocking(false);
			channel.register(this.selector, SelectionKey.OP_WRITE, message);

		} catch (IOException e) {
			e.printStackTrace();
			if (message.getMessage().array()[0] == CommandEnum.GET.getCode())
				ConsistentHash.getFromSuccessor(message.getCoordinator(),
						message.getServerHandle(), message.getMessage(),
						message.getKey());
		}

	}
}
