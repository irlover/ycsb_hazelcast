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
package com.yahoo.ycsb.generator;

import java.util.Random;

import org.apache.log4j.Logger;

/**
 * Generate integers resembling a hotspot distribution where x% of operations
 * access y% of data items. The parameters specify the bounds for the numbers,
 * the percentage of the of the interval which comprises the hot set and
 * the percentage of operations that access the hot set. Numbers of the hot set are
 * always smaller than any number in the cold set. Elements from the hot set and
 * the cold set are chose using a uniform distribution.
 * 
 * @author sudipto
 *
 */
public class HotspotIntegerGenerator extends IntegerGenerator {
	private static Logger LOG = Logger.getLogger(HotspotIntegerGenerator.class);

	private final int lowerBound;
	private final int upperBound;
	private final int hotInterval;
	private final int coldInterval;
	private final double hotsetFraction;
	private final double hotOpnFraction;
	private final Random random;

	/*
	 * 本次调用，热点区起始位置的偏移
	 */
	private int randOffset;
	private static int usedTimes = -1; //第几次被调用

	/**
	 * Create a generator for Hotspot distributions.
	 * 
	 * @param lowerBound lower bound of the distribution.
	 * @param upperBound upper bound of the distribution.
	 * @param hotsetFraction percentage of data item
	 * @param hotOpnFraction percentage of operations accessing the hot set.
	 */
	public HotspotIntegerGenerator(int lowerBound, int upperBound,
			double hotsetFraction, double hotOpnFraction) {
		if (hotsetFraction < 0.0 || hotsetFraction > 1.0) {
			System.err.println("Hotset fraction out of range. Setting to 0.0");
			hotsetFraction = 0.0;
		}
		if (hotOpnFraction < 0.0 || hotOpnFraction > 1.0) {
			System.err
					.println("Hot operation fraction out of range. Setting to 0.0");
			hotOpnFraction = 0.0;
		}
		if (lowerBound > upperBound) {
			System.err
					.println("Upper bound of Hotspot generator smaller than the lower bound. "
							+ "Swapping the values.");
			int temp = lowerBound;
			lowerBound = upperBound;
			upperBound = temp;
		}
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		this.hotsetFraction = hotsetFraction;
		int interval = upperBound - lowerBound + 1;
		this.hotInterval = (int) (interval * hotsetFraction);
		this.coldInterval = interval - hotInterval;
		this.hotOpnFraction = hotOpnFraction;
		random = new Random();

		/**
		 * add by zx，最终选出的key值在原来key值基础上偏移一个随机数。一次测试中本值唯一，每次测试热点数据区不一样
		 */
		/*usedTimes++;
		this.randOffset = (int) (0.3 * usedTimes) % interval;
		LOG.info(String.format("Range: %d-%d, HotSpot offset: %d",
				this.lowerBound, this.upperBound, randOffset));
				*/
	}

	@Override
	public int nextInt() {
		int value = 0;
		if (random.nextDouble() < hotOpnFraction) {
			// Choose a value from the hot set.
			value = lowerBound + random.nextInt(hotInterval);
		} else {
			// Choose a value from the cold set.
			value = lowerBound + hotInterval + random.nextInt(coldInterval);
		}

		//by zx,添加偏移使得每次测试热点数据区不一样。		
		//value = (value + randOffset) % (upperBound - lowerBound + 1);
		//end

		setLastInt(value);
		return value;
	}

	/**
	 * @return the lowerBound
	 */
	public int getLowerBound() {
		return lowerBound;
	}

	/**
	 * @return the upperBound
	 */
	public int getUpperBound() {
		return upperBound;
	}

	/**
	 * @return the hotsetFraction
	 */
	public double getHotsetFraction() {
		return hotsetFraction;
	}

	/**
	 * @return the hotOpnFraction
	 */
	public double getHotOpnFraction() {
		return hotOpnFraction;
	}

}
