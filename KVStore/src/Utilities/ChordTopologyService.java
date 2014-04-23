package Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;

import KVStore.Chord;
import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;

public class ChordTopologyService {

	private static Chord chord = new Chord();

	public ChordTopologyService() {
	}

	public static String getCoordinator(Integer key)
			throws InternalKVStoreFailureException {
		if (chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		int hash = key;
		if (!chord.getChord().containsKey(hash)) {
			SortedMap<Integer, String> tailMap = chord.getChord().tailMap(hash);
			hash = tailMap.isEmpty() ? chord.getChord().firstKey() : tailMap
					.firstKey();
		}

		return chord.getChord().get(hash);
	}

	public static List<String> getCoordinatorAndReplicas(Integer key)
			throws InternalKVStoreFailureException {
		String coord = getCoordinator(key);
		List<String> nodes = new ArrayList<String>();
		nodes.add(coord);

		nodes.addAll(getSuccessors(coord));

		return nodes;
	}

	public static String getSuccessor(String hostName)
			throws InternalKVStoreFailureException {
		return chord.getSuccessor(hostName);
	}

	public static List<String> getSuccessors(String hostName)
			throws InternalKVStoreFailureException {
		return chord.getReplica().get(hostName.hashCode());
	}

	public static List<String> getMySuccessors()
			throws InternalKVStoreFailureException {
		return chord.getSuccessors();
	}

	public static List<String> getAllNodes() {
		return chord.getAllNodes();
	}

	public static List<String> getRandomNodes(int numberOfNodes)
			throws InternalKVStoreFailureException {
		if (chord.getChord().isEmpty())
			throw new InternalKVStoreFailureException();

		Random random = new Random();

		List<String> list = new ArrayList<String>();

		for (int i = 0; i < numberOfNodes; i++) {
			int randomNumber = random.nextInt(chord.getChord().size());
			String randomHost = chord.getNodeByIndex(randomNumber);
			if (!list.contains(randomHost.trim()))
				list.add(randomHost);
		}
		return list;
	}

	public static boolean isNodeExist(Integer hostNameHashCode) {
		return chord.getChord().containsKey(hostNameHashCode);
	}

	public static void handleNodeLeaving(Integer hostNamehashCode) {
		chord.remove(hostNamehashCode);
	}

	public static boolean isSuccessor(String localHost, String remoteHost) {
		String successor = null;
		try {
			successor = getSuccessor(remoteHost);
		} catch (InternalKVStoreFailureException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return localHost.equals(successor);
	}

	public static void announceLeaving(Integer hostNamehashCode) {
		List<String> randomNodes = getAllNodes();

		for (String node : randomNodes) {
			try {
				ConnectionService.connectToGossip(
						CommandEnum.ANNOUNCE_LEAVING.getCode(),
						MessageUtilities.intToByteArray(hostNamehashCode, 32),
						null, node);
			} catch (IOException e) {
				handleNodeLeaving(hostNamehashCode);
				if (randomNodes.indexOf(node) != randomNodes.size() - 1)
					continue;

				e.printStackTrace();
			}
		}
	}

	public static void connectionFailed(String host) {
		int count = chord.getChordFailCount().get(host.hashCode());
		if (count >= 3) {
			announceLeaving(host.hashCode());
			chord.getChordFailCount().put(host.hashCode(), 0);
		} else
			chord.getChordFailCount().put(host.hashCode(), count++);
	}

	/*
	 * public void handleNodeJoining(String hostName) {
	 * this.chord.join(hostName); }
	 */
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
