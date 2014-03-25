package Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import KVStore.Chord;
import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;

public class ChordTopologyService {

	private Chord chord;

	public ChordTopologyService(Chord chord) {
		this.chord = chord;
	}

	public String getNode(String key) throws InexistentKeyException,
			InternalKVStoreFailureException {
		if (this.chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		int hash = key.hashCode();
		System.out.println("PlanetLabNode getNode key hashCode : " + hash);
		if (!this.chord.getChord().containsKey(hash)) {
			SortedMap<Integer, String> tailMap = this.chord.getChord().tailMap(
					hash);
			hash = tailMap.isEmpty() ? this.chord.getChord().firstKey()
					: tailMap.firstKey();
		}

		return this.chord.getChord().get(hash);
	}

	public List<String> getNodes(String fromKey, int numOfReplicas)
			throws InexistentKeyException, InternalKVStoreFailureException {
		if (this.chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		List<String> nodes = new ArrayList<String>();

		int hash = fromKey.hashCode();
		System.out.println("PlanetLabNode getNode key hashCode : " + hash);
		if (!this.chord.getChord().containsKey(hash)) {
			SortedMap<Integer, String> tailMap = this.chord.getChord().tailMap(
					hash);
			hash = tailMap.isEmpty() ? this.chord.getChord().firstKey()
					: tailMap.firstKey();
		}

		nodes.add(this.chord.getChord().get(hash));

		for (int i = 0; i < numOfReplicas - 1; i++) {
			nodes.add(this.getNextNode(hash));
		}

		return nodes;
	}

	private String getNextNode(int key) throws InternalKVStoreFailureException {
		if (this.chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		SortedMap<Integer, String> tailMap = this.chord.getChord().tailMap(key);
		int hash = tailMap.isEmpty() ? this.chord.getChord().firstKey()
				: tailMap.firstKey();

		return this.chord.getChord().get(hash);
	}

	public String getNodeByHostName(String hostName)
			throws InternalKVStoreFailureException {
		if (this.chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		return this.chord.getChord().get(hostName.hashCode());
	}

	public String getNextNodeByHostName(String hostName)
			throws InternalKVStoreFailureException {
		if (this.chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		SortedMap<Integer, String> tailMap = this.chord.getChord().tailMap(
				hostName.hashCode() + 1);
		int hash = tailMap.isEmpty() ? this.chord.getChord().firstKey()
				: tailMap.firstKey();

		return this.chord.getChord().get(hash);
	}

	public List<String> getRandomNodes(String hostName, int numberOfNodes)
			throws InternalKVStoreFailureException {
		if (this.chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		List<String> list = new ArrayList<String>();

		for (int i = 0; i < numberOfNodes; i++) {
			int index = (int) (Math.random() * this.chord.getChord().size());
			String randomHost = this.chord.getNodeByIndex(index);
			if ((!list.contains(randomHost.trim()))
					&& (!randomHost.trim().equals(hostName.trim()))
					&& (!randomHost.trim().equals(
							this.getNextNodeByHostName(hostName.trim()))))
				list.add(randomHost);
		}
		return list;
	}

	public boolean isNodeExist(String ipAddress) {
		return this.chord.getParticipatingNode().containsKey(ipAddress);
	}

	public void handleNodeLeaving(String ipAddress) {
		this.chord.leave(ipAddress);
	}

	public void handleNodeJoining(String ipAddress, String hostName) {
		this.chord.join(ipAddress, hostName);
	}

	public boolean isSuccessor(String localHost, String remoteHost) {
		String successor = null;
		try {
			successor = this.getNextNodeByHostName(remoteHost);
		} catch (InternalKVStoreFailureException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return localHost.equals(successor);
	}
	/*
	 * public byte[] remoteRequest(int command, byte[] key, byte[] value, String
	 * serverHostName) throws InternalKVStoreFailureException { try { byte[]
	 * request = MessageUtilities.formateRequestMessage(
	 * Integer.valueOf(command), key, value);
	 * 
	 * ClientDispatcher dispatcher = new ClientDispatcher();
	 * ReadReplyEventHandler readReplyEventhandler = new ReadReplyEventHandler(
	 * Integer.valueOf(command)); WriteRequestEventHandler
	 * writeRequestEventHandler = new WriteRequestEventHandler(
	 * dispatcher.getDemultiplexer(), ByteBuffer.wrap(request));
	 * ConnectionEventHandler connectionEventHandler = new
	 * ConnectionEventHandler( dispatcher.getDemultiplexer());
	 * 
	 * new ReactorInitiator().initiateReactiveClient(serverHostName, dispatcher,
	 * connectionEventHandler, readReplyEventhandler, writeRequestEventHandler);
	 * 
	 * return readReplyEventhandler.getReplyMessage(); } catch (Exception e) { }
	 * throw new InternalKVStoreFailureException(); }
	 */

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
