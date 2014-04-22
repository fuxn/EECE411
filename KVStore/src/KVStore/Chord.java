package KVStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import Exception.InternalKVStoreFailureException;

public class Chord {

	public static final int PARTICIPATING_NODES = 5;
	public static final int numReplicas = 3;

	private SortedMap<Integer, String> chord = new TreeMap<Integer, String>();
	private List<String> participantingNodes = new ArrayList<String>();
	private List<String> successors = new ArrayList<String>();

	public Chord() {
		
		participantingNodes.add("ricepl-1.cs.rice.edu");
		//participantingNodes.add("planetlab-01.vt.nodes.planet-lab.org");
		participantingNodes.add("planetlab1.csee.usf.edu");
		participantingNodes.add("planetlab3.csee.usf.edu");
		participantingNodes.add("pub1-s.ane.cmc.osaka-u.ac.jp");
		
		//participantingNodes.add("planetlab2.poly.edu");
		//participantingNodes.add("salt.planetlab.cs.umd.edu");
		//participantingNodes.add("miranda.planetlab.cs.umd.edu");
		//participantingNodes.add("planetlab4.csee.usf.edu");
		//participantingNodes.add("planetlab2.sfc.wide.ad.jp");
		
		/*participantingNodes.add("planet-lab4.uba.ar");
		participantingNodes.add("planetlab2.thlab.net");
		participantingNodes.add("planetlab1.cs.uml.edu");
		participantingNodes.add("plab-1.diegm.uniud.it");
		participantingNodes.add("planetlab1.ionio.gr");
		
		participantingNodes.add("plab4.ple.silweb.pl");
		participantingNodes.add("planetlab-3.cs.ucy.ac.cy");
		participantingNodes.add("planet-lab1.cs.ucr.edu");
		participantingNodes.add("planetlab1.cs.pitt.edu");
		participantingNodes.add("planetlab2.cis.upenn.edu");
		
		participantingNodes.add("planetlab2.s3.kth.se");
		participantingNodes.add("planetlab-1.cmcl.cs.cmu.edu");
		participantingNodes.add("planetlab2.science.unitn.it");
		participantingNodes.add("planetlab1.cs.stevens-tech.edu");
		participantingNodes.add("planet-plc-3.mpi-sws.org");
		
		participantingNodes.add("orval.infonet.fundp.ac.be");
		participantingNodes.add("planetlab2.bgu.ac.il");
		participantingNodes.add("kupl2.ittc.ku.edu");
		participantingNodes.add("planet-lab2.uba.ar");
		participantingNodes.add("planetlab2.ifi.uio.no");

		participantingNodes.add("planetlab-1.ing.unimo.it");
		participantingNodes.add("planetlab1.ifi.uio.no");
		participantingNodes.add("planetlab2.eurecom.fr");
		participantingNodes.add("node2.planetlab.mathcs.emory.edu");
		participantingNodes.add("planetlab4.williams.edu");

		participantingNodes.add("planetlab-13.e5.ijs.si");
		participantingNodes.add("planetlab-coffee.ait.ie");
		participantingNodes.add("ple2.tu.koszalin.pl");
		participantingNodes.add("planetlab1.sics.se");
		participantingNodes.add("planetlab1.lkn.ei.tum.de");
		
		participantingNodes.add("aguila2.lsi.upc.edu");
		participantingNodes.add("aguila1.lsi.upc.edu");
		participantingNodes.add("planetlab1.exp-math.uni-essen.de");
		participantingNodes.add("planet1.l3s.uni-hannover.de");
		participantingNodes.add("planetlab1.tmit.bme.hu");
		
		participantingNodes.add("planetlab3.hiit.fi");
		participantingNodes.add("planetlab2.cs.uit.no");
		participantingNodes.add("planetlab-12.e5.ijs.si");
		participantingNodes.add("planetlab4.cs.st-andrews.ac.uk");
		participantingNodes.add("ple2.dmcs.p.lodz.pl");*/

		for (String node : participantingNodes) {
			chord.put(node.hashCode(), node);
		}

		try {
			successors = this.getSuccessors(KVStore.localHost);
		} catch (InternalKVStoreFailureException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (Integer key : chord.keySet()) {
			System.out.println("chord contains : " + chord.get(key));
		}
	}

	public String getSuccessor(String hostName)
			throws InternalKVStoreFailureException {
		if (this.chord.isEmpty())
			throw new InternalKVStoreFailureException();

		SortedMap<Integer, String> tailMap = chord
				.tailMap(hostName.hashCode() + 1);
		int hash = tailMap.isEmpty() ? chord.firstKey() : tailMap.firstKey();

		return this.chord.get(hash);
	}

	public List<String> getSuccessors(String hostName)
			throws InternalKVStoreFailureException {
		List<String> nodes = new ArrayList<String>();
		for (int i = 0; i < numReplicas; i++) {
			hostName = getSuccessor(hostName);
			nodes.add(hostName);
		}

		return nodes;
	}

	public  String getNodeByIndex(int index) {
		if (this.participantingNodes.size() > index)
			return participantingNodes.get(index);

		else
			return participantingNodes.get(0);
	}

	public void remove(Integer hostNameHashCode) {
		if (chord.containsKey(hostNameHashCode)) {
			participantingNodes.remove(chord.get(hostNameHashCode).trim());
			chord.remove(hostNameHashCode);
		}
	}

	public List<String> getAllNodes() {
		return participantingNodes;
	}

	public SortedMap<Integer, String> getChord() {
		return chord;
	}

	public List<String> getSuccessors() {
		return successors;
	}
	/*
	 * public void join(String hostName) { this.chord.put(hostName.hashCode(),
	 * hostName); this.indexs.add(hostName.trim()); }
	 */

	/*
	 * public void partition(List<String> nodes) { Integer hashSpace = (int)
	 * Math.pow(2, 32); Integer nodePartitionSpace = hashSpace / nodes.size();
	 * 
	 * int minHash = -(int) Math.pow(2, 31); int maxHash = (int) Math.pow(2, 31)
	 * - 1;
	 * 
	 * for (String node : nodes) { minHash = minHash * nodes.indexOf(node) + 1;
	 * for (int i = minHash; i < minHash + nodePartitionSpace; i++) {
	 * this.chord.put(i, node); } }
	 * 
	 * System.out.println("chord hashspace : " + maxHash + " contains: " +
	 * this.chord.get(maxHash)); }
	 */
}