package KVStore;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;

import NIO.AcceptEventHandler;
import NIO.Dispatcher;
import NIO.ReactorInitiator;
import NIO.ReadEventHandler;
import NIO.WriteEventHandler;
import NIO_Client.ClientDispatcher;
import NIO_Client.ConnectionEventHandler;
import NIO_Client.ReadReplyEventHandler;
import NIO_Client.WriteRequestEventHandler;
import Utilities.PlanetLabNode;

public class KVStore {

	private static final int NIO_SERVER_PORT = 4560;
	private static Thread serverThread;
	private static Thread clientThread;

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

	public static void main(String[] args) throws IOException {
		String localHostName = InetAddress.getLocalHost().getHostName();
		Collection<String> nodes = new ArrayList<String>();

		nodes.add("planetlab2.cs.ubc.ca");
		nodes.add("planetlab1.cs.ubc.ca");
		// nodes.add("pl-node-1.csl.sri.com");
		nodes.add("planetlab-4.eecs.cwru.edu");
		nodes.add("planetlab-2.cs.auckland.ac.nz");

		// nodes.add("planetlab-2.sysu.edu.cn");
		// nodes.add("planetlab1.acis.ufl.edu");
		// nodes.add("csplanetlab3.kaist.ac.kr");
		// nodes.add("planetlab4.wail.wisc.edu");
		// nodes.add("pl2.eecs.utk.edu");
		//
		// nodes.add("ricepl-5.cs.rice.edu");
		// nodes.add("75-130-96-12.static.oxfr.ma.charter.com");
		// nodes.add("planet-lab4.uba.ar");
		// nodes.add("planetlab2.acis.ufl.edu");
		// nodes.add("planetlab1.cs.uml.edu");
		//
		// nodes.add("planetlab2.buaa.edu.cn");
		// nodes.add("planetlab2.georgetown.edu");
		// nodes.add("planetlab-2.scie.uestc.edu.cn");
		// nodes.add("planetlab-2.usask.ca");
		// nodes.add("planet-lab1.cs.ucr.edu");
		//
		// nodes.add("planetlab1.cs.pitt.edu");
		// nodes.add("planetlab2.cis.upenn.edu");
		// nodes.add("planet-lab2.ufabc.edu.br");
		// nodes.add("planetlab-2.cmcl.cs.cmu.edu");
		// nodes.add("planetlab-1.cmcl.cs.cmu.edu");
		//
		// nodes.add("plonk.cs.uwaterloo.ca");
		// nodes.add("planetlab1.cs.stevens-tech.edu");
		// nodes.add("planet-plc-3.mpi-sws.org");
		// nodes.add("planetlab1.eecs.umich.edu");
		// nodes.add("csplanetlab4.kaist.ac.kr");

		// nodes.add(localHostName);
		System.out.println(localHostName);

		/*
		 * ProtocolImpl protocol = new ProtocolImpl(nodes);
		 * protocol.startServer();
		 */

		try {

			KVStore kvStore = new KVStore();
			kvStore.initiateReactiveServer(localHostName, new Chord(nodes));
			kvStore.initiateReactiveClient();

		} catch (Exception e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					Dispatcher.stop();
					ClientDispatcher.stop();
					KVStore.serverThread.join();
					KVStore.clientThread.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		// server wait for incoming requests;
	}
}
