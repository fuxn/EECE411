package NIO;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import KVStore.Chord;
import NIO_Client.ClientDispatcher;
import NIO_Client.ConnectionEventHandler;
import NIO_Client.ReadReplyEventHandler;
import NIO_Client.WriteRequestEventHandler;

public class ReactorInitiator {

	private static final int NIO_SERVER_PORT = 4560;

	public void initiateReactiveServer(String localHostName,
			Chord chord) throws Exception {
		System.out.println("Starting NIO server at port : " + NIO_SERVER_PORT);

		ServerSocketChannel server = ServerSocketChannel.open();
		server.socket().bind(
				new InetSocketAddress(localHostName, NIO_SERVER_PORT));
		server.configureBlocking(false);

		Dispatcher dispatcher = new Dispatcher();
		dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);

		dispatcher.registerEventHandler(SelectionKey.OP_ACCEPT,
				new AcceptEventHandler(Dispatcher.getDemultiplexer()));

		dispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReadEventHandler(Dispatcher.getDemultiplexer(), chord));

		dispatcher.registerEventHandler(SelectionKey.OP_WRITE,
				new WriteEventHandler());
		
		Thread d = new Thread(dispatcher);
		d.start(); // Run the dispatcher loop

	}

	public void initiateReactiveClient(String hostName,
			ClientDispatcher dispatcher,
			ConnectionEventHandler connectionEventHandler,
			ReadReplyEventHandler readReplyEventhandle,
			WriteRequestEventHandler writeRequestEventHandler) throws Exception {
		System.out.println("Starting new NIO connection to : " + hostName);

		SocketChannel client = SocketChannel.open();
		client.configureBlocking(false);

		client.connect(new InetSocketAddress(hostName, 4560));

		dispatcher.registerChannel(SelectionKey.OP_CONNECT, client);

		dispatcher.registerEventHandler(SelectionKey.OP_CONNECT,
				connectionEventHandler);

		dispatcher.registerEventHandler(SelectionKey.OP_READ,
				readReplyEventhandle);

		dispatcher.registerEventHandler(SelectionKey.OP_WRITE,
				writeRequestEventHandler);

		dispatcher.run();
	}
	
	public void initiateReactiveClient() throws Exception {

		ClientDispatcher clientDispatcher = new ClientDispatcher();
		clientDispatcher.registerEventHandler(
				SelectionKey.OP_CONNECT,
				new ConnectionEventHandler(ClientDispatcher
						.getDemultiplexer()));
		clientDispatcher.registerEventHandler(
				SelectionKey.OP_WRITE,
				new WriteRequestEventHandler(ClientDispatcher
						.getDemultiplexer()));
		clientDispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReadReplyEventHandler());
		
		Thread d = new Thread(clientDispatcher);
		d.start(); 
	}

}
