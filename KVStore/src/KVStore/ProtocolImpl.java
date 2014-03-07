package KVStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.SystemOverloadException;
import Exception.UnrecognizedCommandException;
import Interface.ConsistentHashInterface;
import Utilities.ConsistentHash;
import Utilities.ErrorEnum;
import Utilities.Message;
import Utilities.MessageQueue;
import Utilities.MessageUtilities;
import Utilities.PlanetLabNode;

public class ProtocolImpl {

	private int portNumber = 4560;
	private ConsistentHashInterface cHash;
	private static MessageQueue queue;
	private static ServerSocket serverSocket;

	public ProtocolImpl() {
		Collection<PlanetLabNode> nodes = new ArrayList<PlanetLabNode>();
		try {
			nodes.add(new PlanetLabNode(InetAddress.getLocalHost()
					.getHostName()));
		} catch (UnknownHostException e) {
		}

		this.cHash = new ConsistentHash(1, nodes);
		ProtocolImpl.queue = new MessageQueue();

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
						try {
							onReceiveMessage(client);
						} catch (SystemOverloadException e) {
							OutputStream writer = client.getOutputStream();
							writer.write(ErrorEnum.SYS_OVERLOAD.getCode());
							writer.flush();
						} catch (InternalKVStoreFailureException e) {
							OutputStream writer = client.getOutputStream();
							writer.write(ErrorEnum.INTERNAL_FAILURE.getCode());
							writer.flush();
						}
					}
				} catch (IOException e) {

				}
			}
		})).start();

	}

	public void startExeCommand() {
		new Thread(new ExecuteCommand()).start();
	}

	private static void onReceiveMessage(Socket client)
			throws SystemOverloadException, InternalKVStoreFailureException {

		if (ProtocolImpl.queue.isOverload())
			throw new SystemOverloadException();

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

			ProtocolImpl.queue
					.enqueue(new Message(client, command, key, value));

		} catch (SocketException e) {
			throw new InternalKVStoreFailureException();
		} catch (IOException e) {
			throw new InternalKVStoreFailureException();
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

					System.out.println("new command executed: ");

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
						writer.write(ErrorEnum.INEXISTENT_KEY.getCode());
						writer.flush();
					} catch (UnrecognizedCommandException uc) {
						writer.write(ErrorEnum.UNRECOGNIZED_COMMAND.getCode());
						writer.flush();
					} catch (InternalKVStoreFailureException internalException) {
						writer.write(ErrorEnum.INTERNAL_FAILURE.getCode());
						writer.flush();
					} catch (InvalidKeyException invalideKeyException) {
						writer.write(ErrorEnum.INVALID_KEY.getCode());
						writer.flush();
					} catch (OutOfSpaceException e) {
						writer.write(ErrorEnum.OUT_OF_SPACE.getCode());
						writer.flush();
					}

				} catch (IOException ex) {
					ex.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		private byte[] exec(int command, String key, String value)
				throws InexistentKeyException, UnrecognizedCommandException,
				InternalKVStoreFailureException, InvalidKeyException,
				OutOfSpaceException {
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
