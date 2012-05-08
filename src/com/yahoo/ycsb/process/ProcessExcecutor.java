package com.yahoo.ycsb.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.once.cluster.utils.NetworkUtils;

public class ProcessExcecutor {
	private static Logger LOG = LoggerFactory.getLogger(ProcessExcecutor.class);
	private File processFile = null;
	private String template = "workload_template";
	/**
	 * 根据ip选择不同的目录下的模板
	 */
	private boolean location_aware = false;

	private File templateFile = null;

	public ProcessExcecutor(String processFile) throws FileNotFoundException {
		this(new File(Prop.getProcessConfigFolder(), processFile));
	}

	public ProcessExcecutor(File file) throws FileNotFoundException {
		if (file == null || !file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		this.processFile = file;
	}

	public void excecute() {
		LOG.info(String.format("The process file used is %s",
				this.processFile.getAbsolutePath()));
		List<String> lines = readProcessFile(processFile);
		if (lines.size() == 0) {
			System.err.println(String.format(
					"process file <%s> doesn't contains required data",
					this.processFile));
		}
		String processConfLine = lines.get(0);
		initProcessConfig(processConfLine);

		LOG.info(String.format("start to do a process test: template=%s",
				this.templateFile.getAbsolutePath()));
		for (int i = 1; i < lines.size(); i++) {
			execute(this.templateFile, lines.get(i));
		}
	}

	private void initProcessConfig(String processConfLine) {
		String[] itemArray = processConfLine.split(":");
		for (String item : itemArray) {
			int equalIndex = item.indexOf("=");
			String key = item.substring(0, equalIndex).trim();
			String value = item.substring(equalIndex + 1).trim();

			if (key.equals("template")) {
				template = value;
			} else if (key.equals("location_aware")) {
				location_aware = Boolean.parseBoolean(value);
			} else {
				LOG.error(String.format("Unknown processconf, \"%s=%s\"", key,
						value));
				System.exit(-1);
			}
		}

		File workloadDir = Prop.getWordLoadConfigFolder();
		File templateDir;
		if (location_aware) {
			templateDir = new File(workloadDir, NetworkUtils.getLocalIpAddr());
		} else
			templateDir = workloadDir;

		this.templateFile = new File(templateDir, template);
	}

	/**
	 * 将以#开头的行及空行去掉
	 * 
	 * @param processFile
	 * @return
	 */
	public List<String> readProcessFile(File processFile) {
		List<String> workloadFiles = new ArrayList<String>();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(processFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			while (line != null) {
				String trimedLine = line.trim();
				if (!trimedLine.startsWith("#") && !trimedLine.equals("")) {
					workloadFiles.add(trimedLine);
				}
				line = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return workloadFiles;
	}

	public void execute(File template, String specificConf) {
		WorkLoadExcetutor wlExecutor = new WorkLoadExcetutor(template,
				specificConf);
		wlExecutor.excecute();
	}
}
