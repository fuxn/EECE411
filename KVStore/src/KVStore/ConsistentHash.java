package KVStore;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.UnrecognizedCommandException;
import NIO.Dispatcher;
import NIO.Client.Replica.ReplicaDispatcher;
import Utilities.ChordTopologyService;
import Utilities.CommandEnum;
import Utilities.ConnectionService;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class ConsistentHash {

	private PlanetLabNode local;

	public static int localHostHashCode;

	public static Map<Integer, byte[]> commandQueue = new HashMap<Integer, byte[]>();
	public static Map<Integer, Integer> version = new HashMap<Integer, Integer>();
	public static Map<Integer, Integer> numACK = new HashMap<Integer, Integer>();

	public ConsistentHash() {

		try {
			localHostHashCode = KVStore.localHost.hashCode();
			local = new PlanetLabNode();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void handleAnnouncedFailure() throws InternalKVStoreFailureException {

		Map<Integer, byte[]> keys = this.local.getKeys();
		String nextNode = ChordTopologyService.getSuccessor(KVStore.localHost);
		ChordTopologyService.handleNodeLeaving(localHostHashCode);

		for (Integer key : keys.keySet()) {
			try {
				ConnectionService.connectToGossip(
						CommandEnum.HANDLE_ANNOUNCED_FAILURE.getCode(),
						MessageUtilities.intToByteArray(key, 32),
						keys.get(key), nextNode);
			} catch (IOException e) {
				ChordTopologyService.handleNodeLeaving(nextNode.hashCode());
				e.printStackTrace();
				throw new InternalKVStoreFailureException();
			}
		}
		this.local.removeAll();

		// this.announceDataSent(nextNode);
		this.announceLeaving(localHostHashCode);
	}

	public void announceLeaving(Integer hostNamehashCode)
			throws InternalKVStoreFailureException {
		List<String> randomNodes = ChordTopologyService.getAllNodes();

		for (String node : randomNodes) {
			try {
				ConnectionService.connectToGossip(
						CommandEnum.ANNOUNCE_LEAVING.getCode(),
						MessageUtilities.intToByteArray(hostNamehashCode, 32),
						null, node);
			} catch (IOException e) {
				ChordTopologyService.handleNodeLeaving(hostNamehashCode);
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

	public void handleAnnouncedLeaving(String sender, byte[] hostNameHashCode)
			throws InternalKVStoreFailureException {
		int hash = MessageUtilities.byteArrayToInt(hostNameHashCode);
		if (ChordTopologyService.isNodeExist(hash)) {
			ChordTopologyService.handleNodeLeaving(hash);
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
			else
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

	public void put(Selector selector, SelectionKey handle, byte[] key,
			byte[] value) {
		System.out.println("***************PUT******************");
		Integer keyHash = Arrays.hashCode(key);
		byte[] reply = null;

		try {
			String coord = ChordTopologyService.getCoordinator(keyHash);

			if (coord.trim().equals(KVStore.localHost.trim())) {
				reply = this.local.put(keyHash, value);
				ByteBuffer message = MessageUtilities.requestMessage(
						CommandEnum.PUT_REPLICA.getCode(), key, value);
				putToReplica(coord, handle, message, keyHash);

			} else {
				long s = System.currentTimeMillis();
				ConnectionService.connectToNIORemote(
						coord,
						handle,
						MessageUtilities.requestMessage(
								CommandEnum.PUT_COORD.getCode(), key, value));
				long e = System.currentTimeMillis();
				System.out.print("YF NIO put CONNECT: " + (e - s) + "ms");
				return;
			}

		} catch (InternalKVStoreFailureException e) {
			System.out.println("internal");

			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		} catch (OutOfSpaceException e) {
			System.out.println("outofspace");

			reply = MessageUtilities.formateReplyMessage(ErrorEnum.OUT_OF_SPACE
					.getCode());
		} catch (Exception e) {
			System.out.println("exception");
			e.printStackTrace();

			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		} 
			long s = System.currentTimeMillis();
			Dispatcher.response(handle, reply);
			long e = System.currentTimeMillis();
			System.out.println("YF NIO put response: " + (e - s) + " ms");
		
	}

	public void putCoord(Selector selector, SelectionKey handle, byte[] key,
			byte[] value) {
		Integer keyHash = Arrays.hashCode(key);
		byte[] reply = null;
		try {
			reply = this.local.put(keyHash, value);

			ByteBuffer message = MessageUtilities.requestMessage(
					CommandEnum.PUT_REPLICA.getCode(), key, value);

			putToReplica(KVStore.localHost.trim(), handle, message, keyHash);

		} catch (OutOfSpaceException e) {
			System.out.println("outofspace");

			reply = MessageUtilities.formateReplyMessage(ErrorEnum.OUT_OF_SPACE
					.getCode());
		} catch (Exception e) {
			System.out.println("exception");
			e.printStackTrace();

			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		} 
			long s = System.currentTimeMillis();
			Dispatcher.response(handle, reply);
			long e = System.currentTimeMillis();
			System.out.println("YF NIO put coord response: " + (e - s) + " ms");
		
	}

	public void putReplica(Selector selector, SelectionKey handle, byte[] key,
			byte[] value) {

		System.out.println("***************PUT REPLICA******************");

		Integer keyHash = Arrays.hashCode(key);
		byte[] reply = null;
		try {
			reply = this.local.put(keyHash, value);

		} catch (OutOfSpaceException e) {
			reply = MessageUtilities.formateReplyMessage(ErrorEnum.OUT_OF_SPACE
					.getCode());
		} 
			Dispatcher.response(handle, reply);
		
	}

	public void get(Selector selector, SelectionKey handle, byte[] key,
			byte[] value) {
		Integer keyHash = Arrays.hashCode(key);
		System.out.print("***************GET****************** " + keyHash);
		byte[] replyMessage = null;
		String coords;
		try {
			coords = ChordTopologyService.getCoordinator(keyHash);

			try {

				if (coords.trim().equals(KVStore.localHost.trim())) {
					System.out.println("get local ");
					replyMessage = this.local.get(keyHash);
					System.out.println(replyMessage);
				} else {

					System.out.println("get remote " + handle.isValid());
					ConnectionService.connectToNIORemote(
							coords.trim(),
							handle,
							MessageUtilities.requestMessage(
									CommandEnum.GET.getCode(), key, value));
					return;

				}
			} catch (InexistentKeyException e) {
				replyMessage = MessageUtilities
						.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());
			} catch (InternalKVStoreFailureException e) {
				replyMessage = MessageUtilities
						.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE
								.getCode());
			} catch (Exception e) {
				System.out
						.println("************coord unreachable, get next succsessor********** "
								+ handle.isValid());
				try {
					ConnectionService.connectToNIORemote(
							ChordTopologyService.getSuccessor(coords),
							handle,
							MessageUtilities.requestMessage(
									CommandEnum.GET.getCode(), key, value));
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				return;
			}
		} catch (InternalKVStoreFailureException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
			Dispatcher.response(handle, replyMessage);
		
	}

	public void getReplica(Selector selector, SelectionKey handle, byte[] key,
			byte[] value) {

		Integer keyHash = Arrays.hashCode(key);
		System.out.print("***************GET REPLICA****************** "
				+ keyHash);
		byte[] replyMessage = null;
		try {
			replyMessage = this.local.getReplica(keyHash);
			System.out.println(replyMessage);
		} catch (InexistentKeyException e) {
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());
		} catch (Exception e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		}

			Dispatcher.response(handle, replyMessage);
		
	}

	public void removeReplica(Selector selector, SelectionKey handle,
			byte[] key, byte[] value) {
		Integer keyHash = Arrays.hashCode(key);
		System.out.println("***************REMOVE REPLICA****************** "
				+ keyHash);
		byte[] reply = null;
		try {
			reply = this.local.remove(keyHash);

		} catch (Exception e) {
			reply = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		} 
			Dispatcher.response(handle, reply);
		
	}

	public void remove(Selector selector, SelectionKey handle, byte[] key,
			byte[] value) {

		Integer keyHash = Arrays.hashCode(key);
		System.out
				.println("***************REMOVE****************** " + keyHash);
		byte[] replyMessage = null;
		try {
			List<String> coords = ChordTopologyService
					.getCoordinatorAndReplicas(keyHash);
			if (coords.contains(KVStore.localHost)) {
				replyMessage = this.local.remove(keyHash);
				coords.remove(KVStore.localHost);
				removeFromReplica(coords, handle,
						MessageUtilities.requestMessage(
								CommandEnum.DELETE_REPLICA.getCode(), key,
								value), keyHash);
			} else {
				ConnectionService.connectToNIORemote(
						coords.get(0),
						handle,
						MessageUtilities.requestMessage(
								CommandEnum.DELETE.getCode(), key, value));
				return;
			}

		} catch (InternalKVStoreFailureException e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		} catch (Exception e) {
			e.printStackTrace();
			replyMessage = MessageUtilities
					.formateReplyMessage(ErrorEnum.INTERNAL_FAILURE.getCode());
		}

			Dispatcher.response(handle, replyMessage);
		
	}

	public void putToReplica(final String coord, final SelectionKey handle,
			final ByteBuffer message, final Integer key) throws Exception {

		Thread t = new Thread(new putToReplica(coord, handle, message, key));
		t.start();

	}

	class putToReplica implements Runnable {

		private String coord;
		private SelectionKey handle;
		private ByteBuffer message;
		private Integer key;

		public putToReplica(String coord, SelectionKey handle,
				ByteBuffer message, Integer key) {
			this.coord = coord;
			this.handle = handle;
			this.message = message;
			this.key = key;
		}

		@Override
		public void run() {

			try {
				List<String> nodes = ChordTopologyService.getSuccessors(coord);
				for (String n : nodes) {
					ConnectionService.connectToReplica(n, handle, message, key);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void getFromReplica(List<String> nodes, SelectionKey handle,
			ByteBuffer message, Integer key) throws Exception {
		ReplicaDispatcher.pendingGet.put(handle, null);
		for (String n : nodes) {
			ConnectionService.connectToReplica(n, handle, message, key);
		}
	}

	public static void getFromSuccessor(String coord, SelectionKey handle,
			ByteBuffer message, Integer key) throws Exception {
		String successor = ChordTopologyService.getSuccessor(coord);
		ConnectionService.connectToReplica(successor, handle, message, key);

	}

	public static void removeFromReplica(List<String> nodes,
			SelectionKey handle, ByteBuffer message, Integer key)
			throws Exception {
		for (String n : nodes) {
			ConnectionService.connectToReplica(n, handle, message, key);
		}
	}

}
