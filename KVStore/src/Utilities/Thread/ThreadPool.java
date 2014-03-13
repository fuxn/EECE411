package Utilities.Thread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import Exception.SystemOverloadException;

public class ThreadPool {

	private BlockingQueue<Runnable> taskQueue = null;
	private List<PoolThread> threads = new ArrayList<PoolThread>();
	private boolean isStopped = false;

	public ThreadPool(int noOfThreads, int maxNoOfTasks) {
		this.taskQueue = new ArrayBlockingQueue<Runnable>(maxNoOfTasks);

		for (int i = 0; i < noOfThreads; i++) {
			this.threads.add(new PoolThread(this.taskQueue));
		}
		for (PoolThread thread : this.threads) {
			thread.start();
		}
	}

	public synchronized void execute(Runnable task)
			throws SystemOverloadException {
		if (this.isStopped)
			throw new IllegalStateException("ThreadPool is stopped");

		try{
		this.taskQueue.add(task);
		}catch(IllegalStateException ie){
			throw new SystemOverloadException();
		}
	}

	public synchronized void stop() throws InterruptedException {
		this.isStopped = true;
		for (PoolThread thread : this.threads) {
			thread.toStop();
		}
	}

}
