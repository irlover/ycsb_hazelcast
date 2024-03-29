/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Vector;

import com.yahoo.ycsb.Config;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

class SeriesUnit {
	/**
	 * @param time
	 * @param average
	 */
	public SeriesUnit(long time, double average) {
		this.time = time;
		this.average = average;
	}

	public long time;
	public double average;
}

/**
 * A time series measurement of a metric, such as READ LATENCY.
 */
public class OneMeasurementTimeSeries extends OneMeasurement {
	private static final long serialVersionUID = 6685835576632562181L;

	/**
	 * Granularity for time series; measurements will be averaged in chunks of
	 * this granularity. Units are milliseconds.
	 */

	int _granularity;
	Vector<SeriesUnit> _measurements;

	long start = -1;
	long currentunit = -1;
	long count = 0;
	long sum = 0;
	long operations = 0;
	long totallatency = 0;

	// keep a windowed version of these stats for printing status
	long windowoperations = 0;
	long windowtotallatency = 0;

	int min = -1;
	int max = -1;

	private HashMap<Integer, long[]> returncodes;

	public OneMeasurementTimeSeries(String name) {
		super(name);
		_granularity = Config.getConfig().timeseries_granularity;
		_measurements = new Vector<SeriesUnit>();
		returncodes = new HashMap<Integer, long[]>();
	}

	void checkEndOfUnit(boolean forceend) {
		long now = System.currentTimeMillis();

		if (start < 0) {
			currentunit = 0;
			start = now;
		}

		long unit = ((now - start) / _granularity) * _granularity;

		if ((unit > currentunit) || (forceend)) {
			double avg = ((double) sum) / ((double) count);
			_measurements.add(new SeriesUnit(currentunit, avg));

			currentunit = unit;

			count = 0;
			sum = 0;
		}
	}

	@Override
	public void measure(int latency) {
		checkEndOfUnit(false);

		count++;
		sum += latency;
		totallatency += latency;
		operations++;
		windowoperations++;
		windowtotallatency += latency;

		if (latency > max) {
			max = latency;
		}

		if ((latency < min) || (min < 0)) {
			min = latency;
		}
	}

	@Override
	public void exportMeasurements(MeasurementsExporter exporter)
			throws IOException {
		checkEndOfUnit(true);

		exporter.write(getName(), "Operations", operations);
		exporter.write(getName(), "AverageLatency(ms)",
				(((double) totallatency) / ((double) operations)));
		exporter.write(getName(), "MinLatency(ms)", min);
		exporter.write(getName(), "MaxLatency(ms)", max);

		// TODO: 95th and 99th percentile latency

		for (Integer I : returncodes.keySet()) {
			long[] val = returncodes.get(I);
			exporter.write(getName(), "Return=" + I, val[0]);
		}

		for (SeriesUnit unit : _measurements) {
			exporter.write(getName(), Long.toString(unit.time), unit.average);
		}
	}

	@Override
	public void reportReturnCode(int code) {
		Integer Icode = code;
		if (!returncodes.containsKey(Icode)) {
			long[] val = new long[1];
			val[0] = 0;
			returncodes.put(Icode, val);
		}
		returncodes.get(Icode)[0]++;

	}

	@Override
	public String getSummary() {
		if (windowoperations == 0) {
			return "";
		}
		DecimalFormat d = new DecimalFormat("#.##");
		double report = ((double) windowtotallatency)
				/ ((double) windowoperations);
		windowtotallatency = 0;
		windowoperations = 0;
		return "[" + getName() + " AverageLatency(ms)=" + d.format(report)
				+ "]";
	}

	@Override
	public void add(OneMeasurement m) {
		
	}

	@Override
	public long getOperations() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public HashMap<Integer, long[]> getReturnCodes() {
		// TODO Auto-generated method stub		
		return null;
	}
}
