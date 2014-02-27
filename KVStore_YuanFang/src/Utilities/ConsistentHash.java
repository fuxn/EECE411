package Utilities;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash {

	private final SortedMap<Integer, PlanetLabNode> circle = new TreeMap<Integer, PlanetLabNode>();
	private final int numberOfReplicas;

	public ConsistentHash(int numberOfReplicas, Collection<PlanetLabNode> nodes) {
		this.numberOfReplicas = numberOfReplicas;
		for (PlanetLabNode node : nodes) {
			this.addNode(node);
		}
	}

	public void addNode(PlanetLabNode node) {
		for (int i = 0; i < this.numberOfReplicas; i++) {
			this.circle.put(node.hashCode(), node);
		}
	}

	public PlanetLabNode getNode(Object key) {
		if (this.circle.isEmpty())
			return null;

		int hash = key.hashCode();
		if (!this.circle.containsKey(hash)) {
			SortedMap<Integer, PlanetLabNode> tailMap = this.circle
					.tailMap(hash);
			hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
					.firstKey();
		}
		return this.circle.get(hash);
	}

	public void put(Object key, Object value) {
		PlanetLabNode node = this.getNode(key);

		node.put(key, value);
	}

	public Object get(Object key) {
		PlanetLabNode node = this.getNode(key);
		return node.get(key);
	}

	public void remove(Object key) {
		PlanetLabNode node = this.getNode(key);
		node.remove(key);
	}

}
