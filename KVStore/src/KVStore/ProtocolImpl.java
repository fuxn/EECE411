package KVStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.UnrecognizedCommandException;
import Interface.ConsistentHashInterface;
import Utilities.ConsistentHash;
import Utilities.PlanetLabNode;

public class ProtocolImpl {

	private int portNumber = 4560;
	private ConsistentHashInterface cHash;

	public ProtocolImpl() {
		Collection<PlanetLabNode> nodes = new ArrayList<PlanetLabNode>();
		nodes.add(new PlanetLabNode("pl-node-1.csl.sri.com"));
		nodes.add(new PlanetLabNode("planetlab2.cs.stevens-tech.edu"));
		nodes.add(new PlanetLabNode("planetlab-4.eecs.cwru.edu"));

		this.cHash = new ConsistentHash(3, nodes);
		// this.initializeServer();
	}

	public void initializeServer() {
		try {
			ServerSocket serverSocket = new ServerSocket(this.portNumber);
			System.out.println("Server waiting for client");
			while (true) {
				Socket client = serverSocket.accept();
				new Client(client).start();
			}
		} catch (Exception ie) {
			ie.printStackTrace();
		}
	}

	class Client extends Thread {
		private Socket clientSocket = null;

		public Client(Socket clientSocket) {
			this.clientSocket = clientSocket;
		}

		public void run() {
			try {
				System.out.println("new connection accepted: "
						+ this.clientSocket.getInetAddress());

				InputStream reader = this.clientSocket.getInputStream();
				OutputStream writer = this.clientSocket.getOutputStream();

				int command = reader.read();

				byte[] key = new byte[32];
				int bytesRcvd;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < key.length) {
					if ((bytesRcvd = reader.read(key, totalBytesRcvd,
							key.length - totalBytesRcvd)) == -1)
						throw new SocketException(
								"connection close prematurely.");
					totalBytesRcvd += bytesRcvd;
				}

				byte[] value = new byte[1024];
				bytesRcvd = 0;
				totalBytesRcvd = 0;
				while (totalBytesRcvd < value.length) {
					if ((bytesRcvd = reader.read(value, totalBytesRcvd,
							value.length - totalBytesRcvd)) == -1)
						throw new SocketException(
								"connection close prematurely.");
					totalBytesRcvd += bytesRcvd;
				}

				try {
					byte[] results = this.exec(command, key, value);
					writer.write(results);
					writer.flush();

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
			}
		}

		private byte[] exec(int command, byte[] key, byte[] value)
				throws InexistentKeyException, UnrecognizedCommandException,
				InternalKVStoreFailureException, InvalidKeyException {

			if (command == 1)
				return cHash.put(key, value);
			else if (command == 2)
				value = cHash.get(key);
			else if (command == 3)
				return cHash.remove(key);
			else
				throw new UnrecognizedCommandException();

			System.out.println(command);
			return null;

		}

	}

}
