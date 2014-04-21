package NIO.Client.Replica;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;
import Utilities.Message.RemoteMessage;

public class ConnectReplicaHandler implements EventHandler {
	private Selector selector;

	public ConnectReplicaHandler(Selector demultiplexer) {
		this.selector = demultiplexer;
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel channel = (SocketChannel) handle.channel();
		RemoteMessage message = (RemoteMessage) handle.attachment();
		long endTimeMillis = System.currentTimeMillis() + 5000L;
		while ((!channel.finishConnect())
				&& (System.currentTimeMillis() < endTimeMillis)) {
			System.out.println("pending connection");
		}
		
		channel.configureBlocking(false);
		channel.register(this.selector, SelectionKey.OP_WRITE, message);
	}
}
