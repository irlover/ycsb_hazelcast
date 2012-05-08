package com.yahoo.ycsb.process;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.once.cluster.utils.FileUtils;
import com.yahoo.ycsb.Config;
import com.yahoo.ycsb.client.MasterClient;
import com.yahoo.ycsb.measurements.Measurements;

public class WorkLoadExcetutor {
	private File workloadFile = null;

	private File templateFile = null;
	private String specificConf = null;

	public WorkLoadExcetutor(File templateFile, String specificConf) {
		this.templateFile = templateFile;
		this.specificConf = specificConf;

		Properties myfileprops = readProps(this.templateFile);

		Map<String, String> specificConfMap = getSpecificConf(specificConf);
		for (String key : specificConfMap.keySet()) {
			myfileprops.setProperty(key, specificConfMap.get(key));
		}

		setConfig(myfileprops);
	}

	public WorkLoadExcetutor(File workloadFile) {
		this.workloadFile = workloadFile;
		Properties myfileprops = readProps(workloadFile);
		setConfig(myfileprops);
	}

	private Properties readProps(File workloadFile) {
		Properties myfileprops = null;
		try {
			myfileprops = FileUtils.readProps(workloadFile);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
		return myfileprops;
	}

	private Map<String, String> getSpecificConf(String specificConf2) {
		Map<String, String> map = new HashMap<String, String>();
		String[] itemArray = specificConf2.split(";");
		for (String item : itemArray) {
			int equalIndex = item.indexOf("=");
			String key = item.substring(0, equalIndex).trim();
			String value = item.substring(equalIndex + 1).trim();

			map.put(key, value);
		}

		return map;
	}

	private void setConfig(Properties myfileprops) {
		Config config = new Config();
		for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements();) {
			String prop = (String) e.nextElement();
			config.setProperty(prop, myfileprops.getProperty(prop));
		}
		Config.setConfig(config);
	}

	public void excecute() {
		if (this.workloadFile != null)
			System.out.println("  ===Start test with workload: "
					+ this.workloadFile);
		else
			System.out
					.println(String
							.format("  ===Start test with workload-->template=%s, specific conf=%s",
									this.templateFile.getAbsolutePath(),
									specificConf));

		MasterClient client = MasterClient.getMasterClient();
		//必须添加这行，以清除上一个workload的统计数据
		Measurements.reload();

		client.init();
		client.setupSlaves();
		client.execute();
		client.shutdown();

		if (this.workloadFile != null)
			System.out.println("  ===End test with workload: "
					+ this.workloadFile);
		else
			System.out
					.println(String
							.format("  ===End test with workload-->template=%s, specific conf=%s",
									this.templateFile.getAbsolutePath(),
									specificConf));
	}
}
