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
		
		participantingNodes.add("planetlab2.cs.ubc.ca");
		participantingNodes.add("planetlab1.cs.ubc.ca");
		participantingNodes.add("planetlab-4.imperial.ac.uk");
		participantingNodes.add("planetlab-4.eecs.cwru.edu");
		participantingNodes.add("planetlab-2.cs.auckland.ac.nz");


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