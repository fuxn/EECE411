package KVStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.UnrecognizedCommandException;
import Interface.ConsistentHashInterface;
import Utilities.ConsistentHash;
import Utilities.Message;
import Utilities.MessageQueue;
import Utilities.MessageUtilities;
import Utilities.PlanetLabNode;

public class ProtocolImpl {

	private int portNumber = 4560;
	private ConsistentHashInterface cHash;
	private static MessageQueue queue;
	static ServerSocket serverSocket;

	public ProtocolImpl() {
		Collection<PlanetLabNode> nodes = new ArrayList<PlanetLabNode>();
		nodes.add(new PlanetLabNode("pl-node-1.csl.sri.com"));
		nodes.add(new PlanetLabNode("planetlab2.cs.stevens-tech.edu"));
		nodes.add(new PlanetLabNode("planetlab-4.eecs.cwru.edu"));

		this.cHash = new ConsistentHash(3, nodes);
		ProtocolImpl.queue = new MessageQueue();
		// this.initializeServer();
	}

	public void initializeServer() {

		(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					serverSocket = new ServerSocket(portNumber);
					System.out.println("Server waiting for client");
					while (true) {
						Socket client = serverSocket.accept();
						onReceiveMessage(client);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		})).start();

	}

	public void startExeCommand() {
		new Thread(new ExecuteCommand()).start();
	}

	private static void onReceiveMessage(Socket client) {
		try {
			InputStream reader = client.getInputStream();

			int command = reader.read();
			byte[] key = new byte[32];
			int bytesRcvd;
			int totalBytesRcvd = 0;
			while (totalBytesRcvd < key.length) {
				if ((bytesRcvd = reader.read(key, totalBytesRcvd, key.length
						- totalBytesRcvd)) == -1)
					throw new SocketException("connection close prematurely.");

				totalBytesRcvd += bytesRcvd;
			}

			byte[] value = MessageUtilities.checkRequestValue(command, reader);

			ProtocolImpl.queue.enqueue(new Message(client, command, key, value));

		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private class ExecuteCommand extends Thread {
		private Socket clientSocket = null;
		private Message message;

		public void run() {
			while (true) {
				try {
					this.message = ProtocolImpl.queue.dequeue();
					this.clientSocket = message.getClient();

					System.out.println("new connection accepted: "
							+ this.clientSocket.getInetAddress());

					OutputStream writer = this.clientSocket.getOutputStream();

					try {
						byte[] results = this.exec(this.message.getCommand(),
								this.message.getKey(), this.message.getValue());
						if (results != null) {
							System.out.println("result "
									+ Arrays.toString(results));
							writer.write(results);
							writer.flush();
						}

					} catch (InexistentKeyException ex) {
						writer.write(new byte[] { 0x01 });
						writer.flush();
					} catch (UnrecognizedCommandException uc) {
						writer.write(new byte[] { 0x05 });
						writer.flush();
					} catch (InternalKVStoreFailureException internalException) {
						writer.write(new byte[] { 0x04 });
						writer.flush();
					} catch (InvalidKeyException invalideKeyException) {
						writer.write(new byte[] { 0x21 });
					}

				} catch (IOException ex) {
					ex.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		private byte[] exec(int command, byte[] key, byte[] value)
				throws InexistentKeyException, UnrecognizedCommandException,
				InternalKVStoreFailureException, InvalidKeyException {

			if (command == 1)
				return cHash.put(key, value);
			else if (command == 2)
				return cHash.get(key);
			else if (command == 3)
				return cHash.remove(key);
			else
				throw new UnrecognizedCommandException();
		}

	}

}
