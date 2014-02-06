package Utilities;

import java.util.LinkedList;

/* a synchronized queue */
public class MessageQueue {

	/* the actual queue */
	private LinkedList<Message> _queue;

	/*
	 * the constructor - it simply creates the LinkedList where the queue
	 * elements are stored
	 */
	public MessageQueue() {
		_queue = new LinkedList<Message>();
	}

	/* gets the first element of the queue or blocks if the queue is empty */
	public synchronized Message dequeue() throws InterruptedException {
		while (_queue.isEmpty()) {
			wait();
		}

		return (Message) _queue.removeFirst();
	}

	/* add a new element to the queue */
	public synchronized void enqueue(Message m) {
		_queue.addLast(m);
		notify();
	}
}
