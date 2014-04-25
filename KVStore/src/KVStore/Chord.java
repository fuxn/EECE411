package KVStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import Exception.InternalKVStoreFailureException;

public class Chord {

	public static final int numReplicas = 3;

	private SortedMap<Integer, String> chord = new TreeMap<Integer, String>();
	private Map<Integer, Integer> chordFailCount = new HashMap<Integer, Integer>();
	private List<String> participantingNodes = new ArrayList<String>();
	private List<String> successors = new ArrayList<String>();
	private ConcurrentHashMap<Integer, List<String>> replica = new ConcurrentHashMap<Integer, List<String>>();

	public Chord() {

		participantingNodes.add("pl-node-1.csl.sri.com");
		participantingNodes.add("planetlab2.cs.ubc.ca");
		participantingNodes.add("pl2.eecs.utk.edu");
		participantingNodes.add("planetlab3.cesnet.cz");
		participantingNodes.add("planetlab1.dojima.wide.ad.jp");

		participantingNodes.add("planetlab1.netlab.uky.edu");
		participantingNodes.add("planetlab-4.eecs.cwru.edu");
		participantingNodes.add("planetlab-2.cs.auckland.ac.nz");
		participantingNodes.add("planetlab2.ionio.gr");
		participantingNodes.add("planetlab1.csuohio.edu");

		participantingNodes.add("planetlab4.wail.wisc.edu");
		participantingNodes.add("planetlab-coffee.ait.ie");
		participantingNodes.add("ple2.tu.koszalin.pl");
		participantingNodes.add("planetlab1.exp-math.uni-essen.de");
		participantingNodes.add("planetlab-2.cmcl.cs.cmu.edu");

		participantingNodes.add("planetlab2.cis.upenn.edu");
		participantingNodes.add("planet-lab2.uba.ar");
		participantingNodes.add("planet1.l3s.uni-hannover.de");
		participantingNodes.add("planetlab1.pop-pa.rnp.br");
		participantingNodes.add("planetlab2.dit.upm.es");

		participantingNodes.add("planetvs2.informatik.uni-stuttgart.de");
		participantingNodes.add("planetlab2.csie.nuk.edu.tw");
		participantingNodes.add("planetlab1.tmit.bme.hu");
		participantingNodes.add("plab-1.diegm.uniud.it");
		participantingNodes.add("peeramidion.irisa.fr");

		participantingNodes.add("planetlab-um00.di.uminho.pt");
		participantingNodes.add("host2.planetlab.informatik.tu-darmstadt.de");
		participantingNodes.add("plab4.ple.silweb.pl");
		participantingNodes.add("planetlab-1.imag.fr");
		participantingNodes.add("node2.planetlab.mathcs.emory.edu");

		participantingNodes.add("planetlab1.informatik.uni-goettingen.de");
		participantingNodes.add("planet2.l3s.uni-hannover.de");
		participantingNodes.add("planetlab2.fct.ualg.pt");
		participantingNodes.add("planetlab1.ionio.gr");
		participantingNodes.add("planetlab2.science.unitn.it");

		participantingNodes.add("planetlab1.ifi.uio.no");
		participantingNodes.add("planetlab2.ifi.uio.no");
		participantingNodes.add("planetlab2.eurecom.fr");
		participantingNodes.add("planetlab4.hiit.fi");
		participantingNodes.add("planetlab4.cs.st-andrews.ac.uk");

		participantingNodes.add("planetlab2.lkn.ei.tum.de");
		participantingNodes.add("planetlab2.mta.ac.il");
		participantingNodes.add("planet1.unipr.it");
		participantingNodes.add("pandora.we.po.opole.pl");
		participantingNodes.add("planck227ple.test.ibbt.be");

		participantingNodes.add("lim-planetlab-1.univ-reunion.fr");
		participantingNodes.add("planet2.servers.ua.pt");
		participantingNodes.add("planetlab4.williams.edu");
		participantingNodes.add("planetlab6.goto.info.waseda.ac.jp");
		participantingNodes.add("planetlab2.tamu.edu");

		participantingNodes.add("planet2.unipr.it");
		participantingNodes.add("gschembra3.diit.unict.it");
		participantingNodes.add("planetlab-node3.it-sudparis.eu");
		participantingNodes.add("planetlab1.tsuniv.edu");
		participantingNodes.add("planetlab3.cs.uoregon.edu");

		participantingNodes.add("planetlab1.cti.espol.edu.ec");
		participantingNodes.add("planetlab2.cs.uoregon.edu");
		participantingNodes.add("planetlab3.tamu.edu");
		participantingNodes.add("planetlab1.ucsd.edu");
		participantingNodes.add("pl1snu.koren.kr");

		participantingNodes.add("planetlab4.mini.pw.edu.pl");
		participantingNodes.add("ricepl-4.cs.rice.edu");
		participantingNodes.add("pl1.csl.utoronto.ca");
		participantingNodes.add("planetlab1.eecs.wsu.edu");
		participantingNodes.add("planetlab4.goto.info.waseda.ac.jp");

		participantingNodes.add("planetlab2.cs.unc.edu");
		participantingNodes.add("planetlab1.pop-mg.rnp.br");
		participantingNodes.add("planetlab2.cti.espol.edu.ec");
		participantingNodes.add("planetlab2.williams.edu");
		participantingNodes.add("planetlab1.bgu.ac.il");

		participantingNodes.add("planetlab1.cs.uoregon.edu");
		participantingNodes.add("pl2snu.koren.kr");
		participantingNodes.add("pllx1.parc.xerox.com");
		participantingNodes.add("roam1.cs.ou.edu");
		participantingNodes.add("planetlab-2.research.netlab.hut.fi");

		participantingNodes.add("planetlab1.pjwstk.edu.pl");
		participantingNodes.add("planetlab3.cs.st-andrews.ac.uk");
		participantingNodes.add("pl1.rcc.uottawa.ca");
		participantingNodes.add("pluto.cs.brown.edu");

		participantingNodes.add("plab1.create-net.org");
		participantingNodes.add("pl1.eecs.utk.edu");
		participantingNodes.add("planetlab2.cnis.nyit.edu");
		participantingNodes.add("pl2.cs.yale.edu");
		participantingNodes.add("planetlab1.cs.uit.no");

		participantingNodes.add("planet4.cs.ucsb.edu");
		participantingNodes.add("planetlab1.unr.edu");
		participantingNodes.add("planetlab2.unr.edu");
		participantingNodes.add("planetlab1.temple.edu");
		participantingNodes.add("pl1.cs.yale.edu");

		participantingNodes.add("planetlab1.rutgers.edu");
		participantingNodes.add("planetlab2.xeno.cl.cam.ac.uk");
		participantingNodes.add("planetlab1.nrl.eecs.qmul.ac.uk");
		participantingNodes.add("planetlab2.aut.ac.nz");
		participantingNodes.add("planetlab5.millennium.berkeley.edu");

		participantingNodes.add("planetlab1.cs.umass.edu");
		participantingNodes.add("planetlab2.ucsd.edu");
		participantingNodes.add("planetlab4.cs.uoregon.edu");
		participantingNodes.add("planetlab2.eecs.ucf.edu");
		participantingNodes.add("planet-lab3.uba.ar");

		participantingNodes.add("plab3.eece.ksu.edu");
		participantingNodes.add("planetlab-2.ssvl.kth.se");
		participantingNodes.add("planetlab1.comp.nus.edu.sg");
		participantingNodes.add("planetlab3.williams.edu");
		participantingNodes.add("planet-plc-4.mpi-sws.org");

		for (String node : participantingNodes) {
			chord.put(node.hashCode(), node);
			chordFailCount.put(node.hashCode(), 0);
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

		for (Integer key : chord.keySet()) {
			try {
				replica.put(key, this.getSuccessors(chord.get(key)));
			} catch (InternalKVStoreFailureException e) {
				e.printStackTrace();
			}
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

	public String getNodeByIndex(int index) {
		if (this.participantingNodes.size() > index)
			return participantingNodes.get(index);

		else
			return participantingNodes.get(0);
	}

	public void remove(Integer hostNameHashCode) {
		if (chord.containsKey(hostNameHashCode)) {
			participantingNodes.remove(chord.get(hostNameHashCode).trim());
			chord.remove(hostNameHashCode);
			replica.remove(hostNameHashCode);
		}
		try {
			successors = getSuccessors(KVStore.localHost);
		} catch (InternalKVStoreFailureException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<String> getAllNodes() {
		return participantingNodes;
	}

	public SortedMap<Integer, String> getChord() {
		return chord;
	}

	public Map<Integer, Integer> getChordFailCount() {
		return chordFailCount;
	}

	public List<String> getSuccessors() {
		return successors;
	}

	/*
	 * public void join(String hostName) { this.chord.put(hostName.hashCode(),
	 * hostName); this.indexs.add(hostName.trim()); }
	 */

	public ConcurrentHashMap<Integer, List<String>> getReplica() {
		return replica;
	}

	public void setReplica(ConcurrentHashMap<Integer, List<String>> replica) {
		this.replica = replica;
	}

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