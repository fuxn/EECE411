package KVStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import Exception.InternalKVStoreFailureException;
import NIO.AcceptEventHandler;
import NIO.Dispatcher;
import NIO.ReadEventHandler;
import NIO.WriteEventHandler;
import NIO.Client.ClientDispatcher;
import NIO.Client.ConnectionEventHandler;
import NIO.Client.ReadReplyEventHandler;
import NIO.Client.WriteRequestEventHandler;
import NIO.Client.Replica.ConnectReplicaHandler;
import NIO.Client.Replica.ReadACKEventHandler;
import NIO.Client.Replica.ReplicaDispatcher;
import NIO.Client.Replica.WriteReplicaHandler;
import NIO.Client.Replica.Server.AcceptReplicaHandler;
import NIO.Client.Replica.Server.ReplicaServerDispatcher;
import NIO.Client.Replica.Server.ReplicaServerReadHandler;
import NIO.Client.Replica.Server.ReplicaServerWriteHandler;
import Utilities.ChordTopologyService;
import Utilities.Message.MessageUtilities;
import Utilities.Thread.ThreadPool;

public class KVStore {

	public static final int NIO_SERVER_PORT = 4560;
	public static final int NIO_GOSSIP_PORT = 4590;
	public static final int NIO_REPLICA_PORT = 5010;
	public static final int PARTICIPATING_NODES = 10;

	private static int STATUS = 0;
	public static String localHost;

	private static Thread serverThread;
	private static Thread clientThread;
	private static Thread replicaThread;
	private static Thread replicaServerThread;
	
	private static int maxThreads = 230;
	private static int maxTasks = 40000;
	
	public static Executor threadPool;
	

	public static ConsistentHash cHash;
	
	public KVStore(){
		threadPool = Executors.newFixedThreadPool(maxThreads);
	}

	public void initiateReactiveServer(ConsistentHash cHash) throws Exception {
		System.out.println("Starting NIO server at port : " + NIO_SERVER_PORT);

		ServerSocketChannel server = ServerSocketChannel.open();
		server.socket().bind(
				new InetSocketAddress(KVStore.localHost, NIO_SERVER_PORT));
		server.configureBlocking(false);

		Dispatcher dispatcher = new Dispatcher();
		dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);

		dispatcher.registerEventHandler(SelectionKey.OP_ACCEPT,
				new AcceptEventHandler(Dispatcher.getDemultiplexer()));

		dispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReadEventHandler(Dispatcher.getDemultiplexer(), cHash));

		dispatcher.registerEventHandler(SelectionKey.OP_WRITE,
				new WriteEventHandler());

		serverThread = new Thread(dispatcher);
		serverThread.start(); // Run the dispatcher loop

	}
	
	public void initiateReplicaReactiveServer(ConsistentHash cHash) throws Exception {
		System.out.println("Starting NIO server at port : " + NIO_REPLICA_PORT);

		ServerSocketChannel server = ServerSocketChannel.open();
		server.socket().bind(
				new InetSocketAddress(KVStore.localHost, NIO_REPLICA_PORT));
		server.configureBlocking(false);

		ReplicaServerDispatcher dispatcher = new ReplicaServerDispatcher();
		dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);

		dispatcher.registerEventHandler(SelectionKey.OP_ACCEPT,
				new AcceptReplicaHandler(ReplicaServerDispatcher.getDemultiplexer()));

		dispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReplicaServerReadHandler(ReplicaServerDispatcher.getDemultiplexer(), cHash));

		dispatcher.registerEventHandler(SelectionKey.OP_WRITE,
				new ReplicaServerWriteHandler());

		replicaServerThread = new Thread(dispatcher);
		replicaServerThread.start(); // Run the dispatcher loop

	}

	public void initiateReactiveClient() throws Exception {

		ClientDispatcher clientDispatcher = new ClientDispatcher();
		clientDispatcher
				.registerEventHandler(
						SelectionKey.OP_CONNECT,
						new ConnectionEventHandler(ClientDispatcher
								.getDemultiplexer()));
		clientDispatcher.registerEventHandler(
				SelectionKey.OP_WRITE,
				new WriteRequestEventHandler(ClientDispatcher
						.getDemultiplexer()));
		clientDispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReadReplyEventHandler());

		clientThread = new Thread(clientDispatcher);
		clientThread.start();
	}

	public void initiateReactiveReplica() throws Exception {

		ReplicaDispatcher replicaDispatcher = new ReplicaDispatcher();
		replicaDispatcher
				.registerEventHandler(
						SelectionKey.OP_CONNECT,
						new ConnectReplicaHandler(ReplicaDispatcher
								.getDemultiplexer()));
		replicaDispatcher.registerEventHandler(SelectionKey.OP_WRITE,
				new WriteReplicaHandler(ReplicaDispatcher.getDemultiplexer()));
		replicaDispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReadACKEventHandler());

		replicaThread = new Thread(replicaDispatcher);
		replicaThread.start();
	}

	public void initiateSocketServer(ConsistentHash cHash) throws Exception {
		System.out.println("Starting gossip at port : " + NIO_GOSSIP_PORT);

		ServerSocket serverSocket = new ServerSocket(NIO_GOSSIP_PORT);
		Socket server;

		while (true) {
			server = serverSocket.accept();
			InputStream reader = server.getInputStream();

			int command = reader.read();

			byte[] key = MessageUtilities.checkRequestKey(command, reader);
			byte[] value = MessageUtilities.checkRequestValue(command, reader);

			cHash.execInternal(server, command, key, value);

		}

	}

	public static void checkStatus() {
		STATUS += 1;
		if (STATUS == 3)
			System.exit(0);
	}

	public static void main(String[] args) throws IOException {
		localHost = InetAddress.getLocalHost().getHostName();

		try {

			/*
			 * List<String> nodes = new ArrayList<String>(); Properties prop =
			 * new Properties(); InputStream input = null; try { input = new
			 * FileInputStream("participatingNodes.properties");
			 * prop.load(input); for (int i = 1; i <= PARTICIPATING_NODES; i++)
			 * { nodes.add(prop.getProperty(String.valueOf(i))); } } catch
			 * (IOException ex) { ex.printStackTrace(); } finally { if (input !=
			 * null) { try { input.close(); } catch (IOException e) {
			 * e.printStackTrace(); } } }
			 * 
			 * System.out.println(nodes.size());
			 */

			final KVStore kvStore = new KVStore();
			new ChordTopologyService();
			cHash = new ConsistentHash();
			kvStore.initiateReactiveServer(cHash);
			//kvStore.initiateReactiveClient();
			//kvStore.initiateReactiveReplica();
			kvStore.initiateReplicaReactiveServer(cHash);
			// kvStore.initiateGossipServer(localHostName);

			(new Thread() {
				@Override
				public void run() {
					try {
						kvStore.initiateSocketServer(cHash);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}).start();

		} catch (Exception e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					cHash.handleAnnouncedFailure();
				} catch (InternalKVStoreFailureException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Dispatcher.stop();
				ClientDispatcher.stop();
			}
		});

		// server wait for incoming requests;
	}
}
