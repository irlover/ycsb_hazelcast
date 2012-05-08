package com.yahoo.ycsb.client;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import com.once.cluster.utils.NetworkUtils;
import com.yahoo.ycsb.Config;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.OneMeasurement;
import com.yahoo.ycsb.rmi.SlaveRMIInterface;

public class SlaveClient implements SlaveRMIInterface {
	public static final String REGISTRY_NAME = "SlaveRMIInterface";
	public static final int RMI_PORT = 1099;
	private static SlaveClient client = null;

	private LoadThread lt;
	private Registry registry;

	private SlaveClient() {
		lt = null;

		try {
			SlaveRMIInterface stub = (SlaveRMIInterface) UnicastRemoteObject
					.exportObject(this, 0);
			LocateRegistry.createRegistry(RMI_PORT);
			registry = LocateRegistry.getRegistry();
			registry.rebind(REGISTRY_NAME, stub);
		} catch (Exception e) {
			System.err.println("SlaveRMI interface cannot be created");
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static SlaveClient getSlaveClient() {
		if (client == null)
			client = new SlaveClient();
		return client;
	}

	public HashMap<String, OneMeasurement> getCurrentStats() {
		if (lt != null && lt.getState() != Thread.State.TERMINATED) {
			System.out.println("Ops Done: "
					+ Measurements.getMeasurements().getOperations());
			return Measurements.getMeasurements().getAndResetPartialData();
		} else {
			return null;
		}
	}

	public int setProperties(Config c) {
		if (c == null)
			return -1;
		Config.setConfig(c);
		return 0;
	}

	public int execute() {
		if (lt == null || lt.getState() == Thread.State.TERMINATED) {
			lt = new LoadThread();
		} else {
			return -1;
		}
		lt.start();
		return 0;
	}

	public Thread.State getStatus() {
		if (lt == null)
			return null;
		else
			return lt.getState();
	}

	public void shutdown() {
		try {
			registry.unbind(REGISTRY_NAME);
			UnicastRemoteObject.unexportObject(this, true);
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	public static void main(String args[]) {
		/*	
		 * try {
			InetAddress addr = InetAddress.getLocalHost(); //这句话执行不能成功，会抛异常
					  System.out.println("Binding to: " + addr.getHostAddress());
				    System.setProperty("java.rmi.server.hostname", addr.getHostAddress());
				} catch (UnknownHostException e) {
					System.out.println("I can't get my IP address");
					e.printStackTrace();
				}
				*/

		String addr = NetworkUtils.getLocalIpAddr();
		System.out.println("Binding to: " + addr);
		System.setProperty("java.rmi.server.hostname", addr);

		SlaveClient client = getSlaveClient();
	}
}
