package Utilities;

import java.util.LinkedList;

import Interface.MessageInterface;

/* a synchronized queue */
public class MessageQueue {

	/* the actual queue */
	private LinkedList _queue;

	/*
	 * the constructor - it simply creates the LinkedList where the queue
	 * elements are stored
	 */
	public MessageQueue() {
		_queue = new LinkedList();
	}

	/* gets the first element of the queue or blocks if the queue is empty */
	public synchronized MessageInterface dequeue() throws InterruptedException {
		while (_queue.isEmpty()) {
			wait();
		}

		return (MessageInterface) _queue.removeFirst();
	}

	/* add a new element to the queue */
	public synchronized void enqueue(MessageInterface m) {
		_queue.addLast(m);
		notify();
	}
}
