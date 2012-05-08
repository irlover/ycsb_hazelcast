package com.yahoo.ycsb.client;

import com.yahoo.ycsb.Config;
import com.yahoo.ycsb.DataStore;
import com.yahoo.ycsb.DataStoreException;
import com.yahoo.ycsb.UnknownDataStoreException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.database.DBFactory;
import com.yahoo.ycsb.memcached.MemcachedFactory;

/**
 * A thread pool is a group of a limited number of threads that are used to
 * execute tasks.
 */
public class ClientThreadPool extends ThreadGroup {
	private boolean isAlive;
	private int threadID;
	private int ops;

	/**
	 * Unit: ms
	 */
	private long maxExecutionTime;

	private long start;
	private static int threadPoolID = 0;

	/**
	 * 
	 * @param numThreads
	 * @param ops
	 * @param maxExecutionTime
	 *            单位s
	 * @param workload
	 */
	public ClientThreadPool(int numThreads, int ops, int maxExecutionTime,
			Workload workload) {
		super("ThreadPool-" + (++threadPoolID));
		System.out.println("start threadpool " + threadPoolID + " with "
				+ numThreads + " threads");
		this.ops = ops;
		this.maxExecutionTime = maxExecutionTime * 1000;
		this.start = System.currentTimeMillis();
		setDaemon(true);

		isAlive = true;

		for (int i = 0; i < numThreads; i++) {
			DataStore db = null;
			try {
				if (workload instanceof com.yahoo.ycsb.workloads.MemcachedCoreWorkload)
					db = MemcachedFactory.newMemcached(Config.getConfig().db);
				else if (workload instanceof com.yahoo.ycsb.workloads.DBCoreWorkload)
					db = DBFactory.newDB(Config.getConfig().db);
				else {
					System.out.println("Invalid Database/Workload Combination");
					System.exit(0);
				}
				db.init();
			} catch (UnknownDataStoreException e) {
				System.out
						.println("Unknown DataStore " + Config.getConfig().db);
				System.exit(0);
			} catch (DataStoreException e) {
				e.printStackTrace();
				System.exit(0);
			}
			new PooledThread(workload, db).start();
		}
	}

	protected synchronized boolean getTask() {
		if (!isAlive || ops <= 0 || maxExecutionTimeReached())
			return false;

		ops--;
		return true;
	}

	/**
	 * 是否达到了最大的测试持续时间
	 * 
	 * @return
	 */
	private boolean maxExecutionTimeReached() {
		long now = System.currentTimeMillis();
		return (now - start) >= maxExecutionTime;
	}

	public synchronized void close() {
		if (isAlive) {
			isAlive = false;
			interrupt();
		}
	}

	/**
	 * Closes this ThreadPool and waits for all running threads to finish. Any
	 * waiting tasks are executed.
	 */
	public void join() {
		// notify all waiting threads that this ThreadPool is no
		// longer alive
		/*synchronized (this) {
			isAlive = false;
			notifyAll();
		}*/

		// wait for all threads to finish
		Thread[] threads = new Thread[activeCount()];
		int count = enumerate(threads);
		for (int i = 0; i < count; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ex) {
			}
		}
	}

	/**
	 * A PooledThread is a Thread in a ThreadPool group, designed to run tasks
	 * (Runnables).
	 */
	private class PooledThread extends Thread {
		private Workload workload;
		private DataStore db;

		public PooledThread(Workload workload, DataStore db) {
			super(ClientThreadPool.this, "PooledThread-" + (threadID++));
			this.workload = workload;
			this.db = db;
		}

		public void run() {
			while (!isInterrupted() && getTask()) {
				if (Config.getConfig().do_transactions) {
					workload.doTransaction(db);
				} else {
					workload.doInsert(db);
				}
				//by zx,增加sleep，以使得负载较易控制（能够随线程数增加线性增加）
				try {
					Thread.sleep(Config.getConfig().sleep_time);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// TODO: Probably shouldn't be here
			try {
				db.cleanup();
			} catch (DataStoreException e) {
				e.printStackTrace();
			}

			//System.out.println("Client Thread Done");//by zx
		}
	}
}
