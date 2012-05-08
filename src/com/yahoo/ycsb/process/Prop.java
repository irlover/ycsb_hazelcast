package com.yahoo.ycsb.process;

import java.io.File;

public class Prop {
	private static String home = null;
	private static File baseDir;

	static {
		if (home == null) {
			home = System.getProperty("user.dir");
			System.out.println("user.dir:" + home);
		}

		if (home.endsWith("bin")) {
			home = home.substring(0, home.length() - 4);
		}

		baseDir = new File(home);
		System.out.println("base dir: " + baseDir);
	}

	public static File getBaseDir() {
		return baseDir;
	}

	public static void setBaseDir(File baseDir) {
		Prop.baseDir = baseDir;
	}

	public static File getProcessConfigFolder() {
		File rv = new File(getBaseDir(), "processes");
		return rv;
	}

	public static File getWordLoadConfigFolder() {
		File rv = new File(getBaseDir(), "workloads");
		return rv;
	}

	public static File getExportedResultsFolder() {
		File rv = new File(getBaseDir(), "results");
		return rv;
	}

	public static File getConfDir() {
		File rv = new File(getBaseDir(), "conf");
		return rv;
	}
}
