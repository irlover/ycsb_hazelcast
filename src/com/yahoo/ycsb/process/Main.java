package com.yahoo.ycsb.process;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.log4j.PropertyConfigurator;

public class Main {
	public static void main(String[] args) {
		initLog4j();

		String processFile = "process1"; //default process file
		if (args.length != 0) {
			processFile = args[0];
		}

		ProcessExcecutor pe = null;
		try {
			pe = new ProcessExcecutor(processFile);
			pe.excecute();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void initLog4j() {
		String log4jConfigFile = "log4j.properties";
		File file = new File(Prop.getConfDir(), log4jConfigFile);
		if (file.exists()) {
			PropertyConfigurator.configure(file.getAbsolutePath());
		} else {
			System.err.println("Cannot find Log4j configure file : "
					+ log4jConfigFile);
		}
	}
}
