package KVStore;

import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.UnrecognizedCommandException;
import Interface.ConsistentHashInterface;
import Utilities.ChordTopologyService;
import Utilities.CommandEnum;
import Utilities.ConnectionService;
import Utilities.ErrorEnum;
import Utilities.PlanetLabNode;
import Utilities.Message.MessageUtilities;
import Utilities.Message.Requests;

public class ConsistentHash implements ConsistentHashInterface {

	private ChordTopologyService topologyService;
	private int numberOfReplicas;
	private PlanetLabNode local;

	public ConsistentHash(int numberOfReplicas, Chord chord) {
		this.topologyService = new ChordTopologyService(chord);
		this.numberOfReplicas = numberOfReplicas;

		try {
			String localHost = InetAddress.getLocalHost().getHostName();
			this.local = new PlanetLabNode(localHost);
		} catch (UnknownHostException e) {

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public byte[] put(Integer key, byte[] value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException {
		List<String> nodes = this.topologyService.getNodes(key,
				this.numberOfReplicas);

		String firstNode = nodes.get(0);
		byte[] reply = this.put(firstNode, key, value);
		nodes.remove(0);

		for (String node : nodes) {
			this.put(node, key, value);
		}

		return reply;

	}

	private byte[] put(String node, Integer key, byte[] value)
			throws InexistentKeyException, OutOfSpaceException,
			InternalKVStoreFailureException {

		return this.local.put(key, value);
	}

	public byte[] get(Integer key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		return this.local.get(key);
	}

	public byte[] remove(Integer key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		return this.local.remove(key);
	}

	public void handleAnnouncedFailure() throws InternalKVStoreFailureException {

		Map<Integer, byte[]> keys = this.local.getKeys();
		String nextNode = this.topologyService.getNextNodeByHostName(this.local
				.getHostName());

		for (Integer key : keys.keySet()) {
			try {
				this.topologyService.handleNodeLeaving(this.local.getHostName()
						.hashCode());
				ConnectionService.connectToGossip(
						CommandEnum.HANDLE_ANNOUNCED_FAILURE.getCode(),
						MessageUtilities.intToByteArray(key, 32),
						keys.get(key), nextNode);
			} catch (Exception e) {
				e.printStackTrace();
				throw new InternalKVStoreFailureException();
			}
		}
		this.local.removeAll();

		this.announceDataSent(nextNode);
	}

	public void announceDataSent(String remoteHost)
			throws InternalKVStoreFailureException {
		try {
			ConnectionService.connectToGossip(CommandEnum.DATA_SENT.getCode(),
					null, null, remoteHost);
		} catch (InvalidKeyException e) {
			e.printStackTrace();
			throw new InternalKVStoreFailureException();
		}
	}

	public void announceLeaving(String sender, Integer hostNamehashCode)
			throws InternalKVStoreFailureException {
		List<String> randomNodes = this.topologyService.getRandomNodes(3);

		for (String node : randomNodes) {
			try {
				if (node.equals(this.local.getHostName())
						|| node.equals(sender))
					continue;

				ConnectionService.connectToGossip(
						CommandEnum.ANNOUNCE_LEAVING.getCode(),
						MessageUtilities.intToByteArray(hostNamehashCode, 32),
						null, node);
			} catch (Exception e) {
				if (randomNodes.indexOf(node) != randomNodes.size() - 1)
					continue;

				e.printStackTrace();
			}
		}
	}

	public void handleNeighbourAnnouncedFailure(byte[] key, byte[] value)
			throws InternalKVStoreFailureException, InexistentKeyException,
			OutOfSpaceException, InvalidKeyException {
		this.local.put(MessageUtilities.byteArrayToInt(key), value);
	}

	public void handleNeighbourDataSent(String hostName)
			throws InternalKVStoreFailureException {

		this.handleAnnouncedLeaving(hostName,
				MessageUtilities.intToByteArray(hostName.hashCode(), 32));
	}

	public void handleAnnouncedLeaving(String sender, byte[] hostNameHashCode)
			throws InternalKVStoreFailureException {
		int hash = MessageUtilities.byteArrayToInt(hostNameHashCode);
		if (this.topologyService.isNodeExist(hash)) {
			this.topologyService.handleNodeLeaving(hash);
			this.announceLeaving(sender, hash);
		}
	}

	public void execInternal(Socket socket, int command, byte[] key,
			byte[] value) {
		try {
			if (command == CommandEnum.HANDLE_ANNOUNCED_FAILURE.getCode()) {
				this.handleNeighbourAnnouncedFailure(key, value);
				return;
			} else if (command == CommandEnum.ANNOUNCE_LEAVING.getCode()) {
				this.handleAnnouncedLeaving(socket.getInetAddress()
						.getHostName(), key);
				return;
			} // else if (command == CommandEnum.ANNOUNCE_JOINING.getCode())
				// replyMessage = this.handleAnnouncedJoining(new
				// String(value));

			else if (command == CommandEnum.DATA_SENT.getCode()) {
				this.handleNeighbourDataSent(socket.getInetAddress()
						.getHostName());
				return;
			} else
				throw new UnrecognizedCommandException();

		} catch (InexistentKeyException ex) {
			ex.printStackTrace();
		} catch (UnrecognizedCommandException uc) {
			uc.printStackTrace();
		} catch (InternalKVStoreFailureException internalException) {
			internalException.printStackTrace();
		} catch (InvalidKeyException invalideKeyException) {
			invalideKeyException.printStackTrace();
		} catch (OutOfSpaceException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void execHashOperation(Selector selector, SelectionKey handle,
			int command, byte[] key, byte[] value) {
		byte[] replyMessage = null;
		Integer keyHash = Arrays.hashCode(key);
		try {
			String node = this.topologyService.getNode(keyHash);
			if (!node.equals(this.local.getHostName())) {
				/*
				 * replyMessage = this.remoteRequest(command, key.getBytes(),
				 * value.getBytes(), node);
				 */
				ConnectionService.connectToNIOServer(node, selector, handle,
						MessageUtilities.requestMessage(command, key, value));
			} else {
				if (command == CommandEnum.PUT.getCode()) {
					replyMessage = this.local.put(keyHash, value);
				} else if (command == CommandEnum.GET.getCode()) {
					replyMessage = this.local.get(keyHash);
				} else if (command == CommandEnum.DELETE.getCode()) {
					replyMessage = this.local.remove(keyHash);
				} else
					throw new UnrecognizedCommandException();
			}

		} catch (InexistentKeyException ex) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INEXISTENT_KEY.getCode(), null);
		} catch (InternalKVStoreFailureException internalException) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INTERNAL_FAILURE.getCode(), null);
		} catch (UnrecognizedCommandException uc) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.UNRECOGNIZED_COMMAND.getCode(), null);
		} catch (InvalidKeyException invalideKeyException) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INVALID_KEY.getCode(), null);
		} catch (OutOfSpaceException e) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.OUT_OF_SPACE.getCode(), null);
		} catch (Exception e) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INTERNAL_FAILURE.getCode(), null);
		}

		handle.attach(ByteBuffer.wrap(replyMessage));
		handle.interestOps(SelectionKey.OP_WRITE);
		selector.wakeup();

	}

	/*
	 * public byte[] remoteRequest(int command, byte[] key, byte[] value, String
	 * server) throws InternalKVStoreFailureException, InvalidKeyException {
	 * byte[] reply = null; try {
	 * System.out.println("trying remote request to host " + server); Socket
	 * socket = new Socket(server, KVStore.NIO_GOSSIP_PORT);
	 * 
	 * InputStream in = socket.getInputStream(); OutputStream out =
	 * socket.getOutputStream();
	 * 
	 * byte[] v = MessageUtilities.formateRequestMessage(command, key, value);
	 * out.write(v); out.flush();
	 * 
	 * if (MessageUtilities.isCheckReply(command)) reply =
	 * MessageUtilities.checkReplyValue(command, in); else reply =
	 * MessageUtilities.formateReplyMessage( ErrorEnum.SUCCESS.getCode(), null);
	 * } catch (IOException e) { e.printStackTrace(); throw new
	 * InternalKVStoreFailureException(); } return reply; }
	 */
	/*
	 * public byte[] handleNeighbourAnnouncedJoining(String hostName) throws
	 * InternalKVStoreFailureException, InexistentKeyException,
	 * OutOfSpaceException, InvalidKeyException { if (hostName.length() != 32)
	 * throw new InvalidKeyException("Illegal Key Size.");
	 * 
	 * System.out.println("handling neighbour announced joining @ " +
	 * this.local.getHostName());
	 * 
	 * this.handleAnnouncedJoining(hostName); Map<Integer, byte[]> keys =
	 * this.local.getKeys(Arrays.hashCode(hostName .getBytes())); try { for
	 * (Integer key : keys.keySet()) { this.remoteRequest(
	 * CommandEnum.PUT.getCode(), MessageUtilities.standarizeMessage( new byte[]
	 * { key.byteValue() }, 32), keys.get(key), new String(hostName)); } } catch
	 * (Exception e) { return MessageUtilities.formateReplyMessage(
	 * ErrorEnum.INTERNAL_FAILURE.getCode(), null); }
	 * 
	 * return MessageUtilities.formateReplyMessage( ErrorEnum.SUCCESS.getCode(),
	 * null); }
	 */

	/*
	 * public byte[] handleAnnouncedJoining(String hostName) throws
	 * InternalKVStoreFailureException, InexistentKeyException,
	 * OutOfSpaceException, InvalidKeyException { if
	 * (!this.topologyService.isNodeExist(hostName)) {
	 * System.out.println("announcing joing " + hostName);
	 * this.announceJoining(hostName);
	 * this.topologyService.handleNodeJoining(hostName);
	 * 
	 * if (this.topologyService.isSuccessor(this.local.getHostName(), hostName))
	 * this.handleNeighbourAnnouncedJoining(hostName);
	 * 
	 * return MessageUtilities.formateReplyMessage( ErrorEnum.SUCCESS.getCode(),
	 * null); } else { System.out.println("already exist"); return
	 * MessageUtilities.formateReplyMessage( ErrorEnum.SUCCESS.getCode(), null);
	 * } }
	 */

	/*
	 * public byte[] announceJoining(String hostName) throws
	 * InternalKVStoreFailureException { List<String> randomNodes =
	 * this.topologyService.getRandomNodes(3); for (String node : randomNodes) {
	 * try { if (node.equals(this.local.getHostName())) continue;
	 * System.out.println("announce joining to host:" + node);
	 * this.remoteRequest(CommandEnum.ANNOUNCE_JOINING.getCode(), null,
	 * MessageUtilities.standarizeMessage( hostName.getBytes(), 1024), node); }
	 * catch (Exception e) { if (randomNodes.indexOf(node) != randomNodes.size()
	 * - 1) continue; else return MessageUtilities.formateReplyMessage(
	 * ErrorEnum.INTERNAL_FAILURE.getCode(), null); } } return
	 * MessageUtilities.formateReplyMessage( ErrorEnum.SUCCESS.getCode(), null);
	 * }
	 */

}
