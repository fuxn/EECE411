package Utilities;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import NIO.ReactorInitiator;
import NIO_Client.ClientDispatcher;
import NIO_Client.ConnectionEventHandler;
import NIO_Client.ReadReplyEventHandler;
import NIO_Client.WriteRequestEventHandler;
import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;

public class LookupService {
	private SortedMap<Integer, String> circle = new TreeMap<Integer, String>();

	public LookupService(int numberOfReplicas, Collection<String> nodes) {
		for (String node : nodes) {
			this.circle.put(node.hashCode(), node);
		}
	}

	public String getNode(String key) throws InexistentKeyException,
			InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		int hash = key.hashCode();
		System.out.println("PlanetLabNode getNode key hashCode : " + hash);
		if (!this.circle.containsKey(hash)) {
			SortedMap<Integer, String> tailMap = this.circle.tailMap(hash);
			hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
					.firstKey();
		}

		return this.circle.get(hash);
	}

	public List<String> getNodes(String fromKey, int numOfReplicas)
			throws InexistentKeyException, InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		List<String> nodes = new ArrayList<String>();

		int hash = fromKey.hashCode();
		System.out.println("PlanetLabNode getNode key hashCode : " + hash);
		if (!this.circle.containsKey(hash)) {
			SortedMap<Integer, String> tailMap = this.circle.tailMap(hash);
			hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
					.firstKey();
		}

		nodes.add(this.circle.get(hash));

		for (int i = 0; i < numOfReplicas - 1; i++) {
			nodes.add(this.getNextNode(hash));
		}

		return nodes;
	}

	private String getNextNode(int key) throws InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		SortedMap<Integer, String> tailMap = this.circle.tailMap(key);
		int hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
				.firstKey();

		return this.circle.get(hash);
	}

	public String getNodeByHostName(String hostName)
			throws InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		return this.circle.get(hostName.hashCode());
	}

	public String getNextNodeByHostName(String hostName)
			throws InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		SortedMap<Integer, String> tailMap = this.circle.tailMap(hostName
				.hashCode() + 1);
		int hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
				.firstKey();

		return this.circle.get(hash);
	}

	public byte[] remoteRequest(int command, byte[] key, byte[] value,
			String serverHostName) throws InternalKVStoreFailureException {
		try {
			byte[] request = MessageUtilities.formateRequestMessage(
					Integer.valueOf(command), key, value);

			ClientDispatcher dispatcher = new ClientDispatcher();
			ReadReplyEventHandler readReplyEventhandler = new ReadReplyEventHandler();
			WriteRequestEventHandler writeRequestEventHandler = new WriteRequestEventHandler(
					dispatcher.getDemultiplexer(), ByteBuffer.wrap(request));
			ConnectionEventHandler connectionEventHandler = new ConnectionEventHandler(
					dispatcher.getDemultiplexer());

			new ReactorInitiator().initiateReactiveClient(serverHostName,
					dispatcher, connectionEventHandler, readReplyEventhandler,
					writeRequestEventHandler);

			return readReplyEventhandler.getReplyMessage();
		} catch (Exception e) {
		}
		throw new InternalKVStoreFailureException();
	}

	/*
	 * public byte[] remoteRequest(int command, byte[] key, byte[] value, String
	 * server) throws InternalKVStoreFailureException { byte[] reply = null; try
	 * { Socket socket = new Socket(server, 4560);
	 * System.out.println("Connecting to : " + socket.getInetAddress());
	 * System.out.println("connecting to server..");
	 * 
	 * InputStream in = socket.getInputStream(); OutputStream out =
	 * socket.getOutputStream();
	 * 
	 * byte[] v = MessageUtilities.formateRequestMessage(command, key, value);
	 * out.write(v); out.flush();
	 * 
	 * reply = MessageUtilities.checkReplyValue(command, in); } catch
	 * (IOException e) {
	 * 
	 * throw new InternalKVStoreFailureException(); } return reply; }
	 */

}
