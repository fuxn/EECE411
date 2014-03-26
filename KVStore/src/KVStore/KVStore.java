package KVStore;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;

import NIO.AcceptEventHandler;
import NIO.Dispatcher;
import NIO.ReadEventHandler;
import NIO.WriteEventHandler;
import NIO_Client.ClientDispatcher;
import NIO_Client.ConnectionEventHandler;
import NIO_Client.ReadReplyEventHandler;
import NIO_Client.WriteRequestEventHandler;
import NIO_Gossip.ConnectionGossipHandler;
import NIO_Gossip.GossipDispatcher;
import NIO_Gossip.ReadGossipHandler;
import NIO_Gossip.WriteGossipHandler;

public class KVStore {

	private static final int NIO_SERVER_PORT = 4560;
	private static final int NIO_GOSSIP_PORT = 4590;

	private static int STATUS = 0;

	private static Thread serverThread;
	private static Thread clientThread;
	private static Thread gossipThread;

	public void initiateReactiveServer(String localHostName, Chord chord)
			throws Exception {
		System.out.println("Starting NIO server at port : " + NIO_SERVER_PORT);

		ServerSocketChannel server = ServerSocketChannel.open();
		server.socket().bind(
				new InetSocketAddress(localHostName, NIO_SERVER_PORT));
		server.configureBlocking(false);

		Dispatcher dispatcher = new Dispatcher();
		dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);

		dispatcher.registerEventHandler(SelectionKey.OP_ACCEPT,
				new AcceptEventHandler(Dispatcher.getDemultiplexer()));

		dispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReadEventHandler(Dispatcher.getDemultiplexer(), chord));

		dispatcher.registerEventHandler(SelectionKey.OP_WRITE,
				new WriteEventHandler());

		serverThread = new Thread(dispatcher);
		serverThread.start(); // Run the dispatcher loop

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

	public void initiateGossipServer(String localHostName) throws Exception {
		System.out.println("Starting NIO gossip at port : " + NIO_GOSSIP_PORT);

		ServerSocketChannel server = ServerSocketChannel.open();
		server.socket().bind(
				new InetSocketAddress(localHostName, NIO_GOSSIP_PORT));
		server.configureBlocking(false);

		GossipDispatcher dispatcher = new GossipDispatcher();
		dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);

		dispatcher.registerEventHandler(SelectionKey.OP_ACCEPT,
				new AcceptEventHandler(GossipDispatcher.getDemultiplexer()));

		dispatcher.registerEventHandler(SelectionKey.OP_READ,
				new ReadGossipHandler(GossipDispatcher.getDemultiplexer()));

		dispatcher.registerEventHandler(SelectionKey.OP_WRITE,
				new WriteGossipHandler(GossipDispatcher.getDemultiplexer()));
		dispatcher
				.registerEventHandler(
						SelectionKey.OP_CONNECT,
						new ConnectionGossipHandler(GossipDispatcher
								.getDemultiplexer()));

		gossipThread = new Thread(dispatcher);
		gossipThread.start(); // Run the dispatcher loop

	}

	public static void checkStatus() {
		STATUS += 1;
		if (STATUS == 3)
			System.exit(0);
	}

	public static void main(String[] args) throws IOException {
		String localHostName = InetAddress.getLocalHost().getHostName();
		List<String> nodes = new ArrayList<String>();

		nodes.add("planetlab2.cs.ubc.ca");
		nodes.add("planetlab1.cs.ubc.ca");
		/*
		 * nodes.add("pl-node-1.csl.sri.com");
		 * nodes.add("planetlab-4.eecs.cwru.edu");
		 * nodes.add("planetlab-2.cs.auckland.ac.nz");
		 * 
		 * nodes.add("planetlab-2.sysu.edu.cn");
		 * nodes.add("planetlab1.acis.ufl.edu"); nodes.add("pl2.eecs.utk.edu");
		 * nodes.add("ricepl-5.cs.rice.edu"); nodes.add("planetlab2.s3.kth.se");
		 * 
		 * nodes.add("planet-lab4.uba.ar");
		 * nodes.add("planetlab2.acis.ufl.edu");
		 * nodes.add("planetlab1.cs.uml.edu");
		 * nodes.add("planetlab2.buaa.edu.cn");
		 * nodes.add("planetlab2.georgetown.edu");
		 * 
		 * nodes.add("planetlab-2.scie.uestc.edu.cn");
		 * nodes.add("planetlab-2.usask.ca");
		 * nodes.add("planet-lab1.cs.ucr.edu");
		 * nodes.add("planetlab1.cs.pitt.edu");
		 * nodes.add("planetlab2.cis.upenn.edu");
		 * 
		 * nodes.add("planetlab-2.cmcl.cs.cmu.edu");
		 * nodes.add("planetlab-1.cmcl.cs.cmu.edu");
		 * nodes.add("plonk.cs.uwaterloo.ca");
		 * nodes.add("planetlab1.cs.stevens-tech.edu");
		 * nodes.add("planet-plc-3.mpi-sws.org");
		 * 
		 * nodes.add("planetlab1.eecs.umich.edu");
		 * nodes.add("planetlab2.bgu.ac.il"); nodes.add("kupl2.ittc.ku.edu");
		 * nodes.add("planet-lab2.uba.ar"); nodes.add("pl2.pku.edu.cn");
		 * 
		 * nodes.add("planetlab1.pop-pa.rnp.br"); nodes.add("pln.zju.edu.cn");
		 * nodes.add("planetlab-1.sjtu.edu.cn");
		 * nodes.add("node2.planetlab.mathcs.emory.edu");
		 * nodes.add("planetlab4.williams.edu");
		 * 
		 * nodes.add("planetlab-13.e5.ijs.si");
		 * nodes.add("planetlab-coffee.ait.ie");
		 * nodes.add("ple2.tu.koszalin.pl"); nodes.add("planetlab1.sics.se");
		 * nodes.add("planetlab1.lkn.ei.tum.de");
		 * 
		 * nodes.add("aguila2.lsi.upc.edu"); nodes.add("aguila1.lsi.upc.edu");
		 * nodes.add("planetlab1.exp-math.uni-essen.de");
		 * nodes.add("planet1.l3s.uni-hannover.de");
		 * nodes.add("planetlab1.tmit.bme.hu");
		 * 
		 * nodes.add("planetlab3.hiit.fi"); nodes.add("planetlab2.cs.uit.no");
		 */
		nodes.add("planetlab-12.e5.ijs.si");
		nodes.add("planetlab4.cs.st-andrews.ac.uk");
		nodes.add("planetlab-4.imperial.ac.uk");

		// nodes.add(localHostName);
		// System.out.println(localHostName);

		/*
		 * ProtocolImpl protocol = new ProtocolImpl(nodes);
		 * protocol.startServer();
		 */

		try {

			KVStore kvStore = new KVStore();
			kvStore.initiateReactiveServer(localHostName, new Chord(nodes));
			kvStore.initiateReactiveClient();
			kvStore.initiateGossipServer(localHostName);

		} catch (Exception e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				Dispatcher.stop();
				ClientDispatcher.stop();
				GossipDispatcher.stop();
				/*
				 * try { //KVStore.serverThread.join(); } catch
				 * (InterruptedException e) { // TODO Auto-generated catch block
				 * e.printStackTrace(); } try { //KVStore.clientThread.join(); }
				 * catch (InterruptedException e) { // TODO Auto-generated catch
				 * block e.printStackTrace(); }
				 */

			}
		});

		// server wait for incoming requests;
	}
}
