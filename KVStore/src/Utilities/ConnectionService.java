package Utilities;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import Exception.SystemOverloadException;
import KVStore.KVStore;
import NIO.Client.ClientDispatcher;
import NIO.Client.ConnectToRemoteNode;
import NIO.Client.Replica.ConnectToReplica;
import NIO.Client.Replica.ReplicaDispatcher;
import Utilities.Message.MessageUtilities;

public class ConnectionService {

	public static void connectToNIORemote(String host, SelectionKey handle,
			ByteBuffer message) throws Exception {
		SocketChannel client;
		try {
			client = SocketChannel.open();

			System.out.println("connect to nio remote " + handle.isValid());
			client.configureBlocking(false);
			client.connect(new InetSocketAddress(host, KVStore.NIO_SERVER_PORT));
			ClientDispatcher.registerChannel(SelectionKey.OP_CONNECT, client,
					handle, message, host);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void connectToSocketRemote(String host, SelectionKey handle, byte[] message, boolean waitForReply){
		
			KVStore.threadPool.execute(new ConnectToRemoteNode(host,message,handle,waitForReply));
		
	}
	
	public static void connectToSocketReplica(String host, SelectionKey handle, byte[] message, boolean waitForReply){
		
			KVStore.threadPool.execute(new ConnectToReplica(host,message,handle,waitForReply));
		
	}

	public static void connectToReplica(String host, SelectionKey handle,
			ByteBuffer message, Integer key) throws Exception {

		SocketChannel client = SocketChannel.open();

		client.configureBlocking(false);
		client.connect(new InetSocketAddress(host, KVStore.NIO_REPLICA_PORT));
		ReplicaDispatcher.registerChannel(SelectionKey.OP_CONNECT, client, key,
				message, handle, host);

	}

	public static void connectToGossip(int command, byte[] key, byte[] value,
			String server) throws IOException {
		Socket socket = new Socket(server, KVStore.NIO_GOSSIP_PORT);

		OutputStream out = socket.getOutputStream();

		byte[] v = MessageUtilities.formateRequestMessage(command, key, value);
		out.write(v);
		out.flush();
	}

}
