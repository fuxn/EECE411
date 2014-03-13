package Utilities.Thread;

import java.util.concurrent.BlockingQueue;

public class PoolThread extends Thread {

	private BlockingQueue<Runnable> taskQueue = null;
	private boolean isStopped = false;

	public PoolThread(BlockingQueue<Runnable> queue) {
		this.taskQueue = queue;
	}

	public void run() {
		while (!isStopped()) {
			try {
				Runnable runnable = (Runnable) this.taskQueue.poll();
				runnable.run();
			} catch (Exception e) {
				// log or otherwise report exception,
				// but keep pool thread alive.
			}
		}
	}

	public synchronized void toStop() throws InterruptedException {
		this.isStopped = true;
		this.interrupt(); // break pool thread out of dequeue() call.
		this.join();
	}

	public synchronized boolean isStopped() {
		return this.isStopped;
	}
}