package Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import KVStore.KVStore;
import NIO_Client.ClientDispatcher;
import Utilities.Message.MessageUtilities;

public class ConnectionService {

	public static void connectToNIOServer(String host, Selector selector,
			SelectionKey handle, ByteBuffer message) throws Exception {
		System.out.println("connect remote server : " + host);
		SocketChannel client;
		try {
			client = SocketChannel.open();

			client.configureBlocking(false);
			client.connect(new InetSocketAddress(host, KVStore.NIO_SERVER_PORT));
			ClientDispatcher.registerChannel(SelectionKey.OP_CONNECT, client,
					handle, message);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void connectToGossip(int command, byte[] key, byte[] value,
			String server) throws InternalKVStoreFailureException,
			InvalidKeyException {
		try {
			System.out.println("trying remote request to host " + server);
			Socket socket = new Socket(server, KVStore.NIO_GOSSIP_PORT);

			OutputStream out = socket.getOutputStream();

			byte[] v = MessageUtilities.formateRequestMessage(command, key,
					value);
			out.write(v);
			out.flush();

		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalKVStoreFailureException();
		}
	}

}