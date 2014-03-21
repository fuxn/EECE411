package KVStore;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

import NIO.ReactorInitiator;
import Utilities.PlanetLabNode;

public class KVStore {

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
			new ReactorInitiator().initiateReactiveServer(localHostName,
					new Chord(nodes));
			new ReactorInitiator().initiateReactiveClient();
		} catch (Exception e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Server Closing");
			}
		});

		// server wait for incoming requests;
	}
}
