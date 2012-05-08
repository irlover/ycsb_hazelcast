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

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.ChurnGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.memcached.Memcached;

/**
 * work load that performs get, set operations on a key-value store
 * @author zhuxin
 */
public class KeyValueCoreWorkload extends Workload {

	IntegerGenerator keysequence;

	DiscreteGenerator operationchooser;

	IntegerGenerator keychooser;

	Generator fieldchooser;

	CounterGenerator transactioninsertkeysequence;

	IntegerGenerator scanlength;

	boolean orderedinserts;

	/**
	 * Initialize the scenario. Called once, in the main client thread, before
	 * any operations are started.
	 */
	public void init() throws WorkloadException {
		int recordcount = Config.getConfig().record_count;
		int insertstart = Config.getConfig().insert_start;

		if (Config.getConfig().insert_order.compareTo("hashed") == 0) {
			orderedinserts = false;
		} else {
			orderedinserts = true;
		}

		keysequence = new CounterGenerator(insertstart);
		operationchooser = new DiscreteGenerator();

		if (Config.getConfig().memadd_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memadd_proportion,
					"ADD");
		}
		if (Config.getConfig().memappend_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memappend_proportion,
					"APPEND");
		}
		if (Config.getConfig().memcas_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memcas_proportion,
					"CAS");
		}
		if (Config.getConfig().memdecr_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memdecr_proportion,
					"DECR");
		}
		if (Config.getConfig().memdelete_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memdelete_proportion,
					"DELETE");
		}
		if (Config.getConfig().memget_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memget_proportion,
					"GET");
		}
		if (Config.getConfig().memgets_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memgets_proportion,
					"GETS");
		}
		if (Config.getConfig().memincr_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memincr_proportion,
					"INCR");
		}
		if (Config.getConfig().memprepend_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memprepend_proportion,
					"PREPEND");
		}
		if (Config.getConfig().memreplace_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memreplace_proportion,
					"REPLACE");
		}
		if (Config.getConfig().memset_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memset_proportion,
					"SET");
		}
		if (Config.getConfig().memupdate_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memupdate_proportion,
					"UPDATE");
		}

		transactioninsertkeysequence = new CounterGenerator(recordcount);
		if (Config.getConfig().request_distribution.compareTo("uniform") == 0) {
			keychooser = new UniformIntegerGenerator(0, recordcount - 1);
		} else if (Config.getConfig().request_distribution.compareTo("zipfian") == 0) {
			// it does this by generating a random "next key" in part by taking
			// the modulus over the number of keys
			// if the number of keys changes, this would shift the modulus, and
			// we don't want that to change which keys are popular
			// so we'll actually construct the scrambled zipfian generator with
			// a keyspace that is larger than exists at the beginning
			// of the test. that is, we'll predict the number of inserts, and
			// tell the scrambled zipfian generator the number of existing keys
			// plus the number of predicted keys as the total keyspace. then, if
			// the generator picks a key that hasn't been inserted yet, will
			// just ignore it and pick another key. this way, the size of the
			// keyspace doesn't change from the perspective of the scrambled
			// zipfian generator

			int opcount = Config.getConfig().operation_count;
			int expectednewkeys = (int) (((double) opcount)
					* Config.getConfig().memset_proportion * 2.0); // 2 is fudge
			// factor
			keychooser = new ScrambledZipfianGenerator(recordcount
					+ expectednewkeys);
		} else if (Config.getConfig().request_distribution.compareTo("latest") == 0) {
			keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
		} else if (Config.getConfig().request_distribution.compareTo("churn") == 0) {
			keychooser = new ChurnGenerator(Config.getConfig().working_set,
					Config.getConfig().churn_delta, recordcount);
		} else if (Config.getConfig().request_distribution.compareTo("hotspot") == 0) {
			//			keychooser = new HotspotIntegerGenerator(0, recordcount - 1, 0.2,//默认20%的数据具有80%的访问量
			//					0.8);
			keychooser = new HotspotIntegerGenerator(0, recordcount - 1, 0.1,//增加热度
					0.9);

		} else {
			throw new WorkloadException("Unknown distribution \""
					+ Config.getConfig().request_distribution + "\"");
		}

		fieldchooser = new UniformIntegerGenerator(0,
				Config.getConfig().field_count - 1);

		if (Config.getConfig().scan_length_distribution.compareTo("uniform") == 0) {
			scanlength = new UniformIntegerGenerator(1,
					Config.getConfig().max_scan_length);
		} else if (Config.getConfig().scan_length_distribution
				.compareTo("zipfian") == 0) {
			scanlength = new ZipfianGenerator(1,
					Config.getConfig().max_scan_length);
		} else {
			throw new WorkloadException("Distribution \""
					+ Config.getConfig().scan_length_distribution
					+ "\" not allowed for scan length");
		}
	}

	/**
	 * Do one insert operation. Because it will be called concurrently from
	 * multiple client threads, this function must be thread safe. However,
	 * avoid synchronized, or the threads will block waiting for each other, and
	 * it will be difficult to reach the target throughput. Ideally, this
	 * function would have no side effects other than DB operations.
	 */
	public boolean doInsert(DataStore memcached) {
		int keynum = keysequence.nextInt();
		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String dbkey = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);

		if (((Memcached) memcached).set(dbkey, value) == 0)
			return true;
		else
			return false;
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from
	 * multiple client threads, this function must be thread safe. However,
	 * avoid synchronized, or the threads will block waiting for each other, and
	 * it will be difficult to reach the target throughput. Ideally, this
	 * function would have no side effects other than DB operations.
	 */
	public boolean doTransaction(DataStore memcached) {
		String op = operationchooser.nextString();

		if (op.compareTo("ADD") == 0) {
			doTransactionAdd((Memcached) memcached);
		} else if (op.compareTo("APPEND") == 0) {
			doTransactionAppend((Memcached) memcached);
		} else if (op.compareTo("CAS") == 0) {
			doTransactionCas((Memcached) memcached);
		} else if (op.compareTo("DECR") == 0) {
			doTransactionDecr((Memcached) memcached);
		} else if (op.compareTo("DELETE") == 0) {
			doTransactionDelete((Memcached) memcached);
		} else if (op.compareTo("GET") == 0) {
			doTransactionGet((Memcached) memcached);
		} else if (op.compareTo("GETS") == 0) {
			doTransactionGets((Memcached) memcached);
		} else if (op.compareTo("INCR") == 0) {
			doTransactionIncr((Memcached) memcached);
		} else if (op.compareTo("PREPEND") == 0) {
			doTransactionPrepend((Memcached) memcached);
		} else if (op.compareTo("REPLACE") == 0) {
			doTransactionReplace((Memcached) memcached);
		} else if (op.compareTo("SET") == 0) {
			doInsert((Memcached) memcached);
		} else if (op.compareTo("UPDATE") == 0) {
			doTransactionUpdate((Memcached) memcached);
		}
		return true;
	}

	public void doTransactionAdd(Memcached memcached) {
		// choose the next key
		int keynum = transactioninsertkeysequence.nextInt();
		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String dbkey = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.add(dbkey, value);
	}

	public void doTransactionAppend(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		memcached.append(key, 0, "appended_string");
	}

	public void doTransactionCas(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		long cas = memcached.gets(key);
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.cas(key, cas, value);
	}

	public void doTransactionDecr(Memcached memcached) {

	}

	public void doTransactionDelete(Memcached memcached) {

	}

	public void doTransactionGet(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = Config.getConfig().key_prefix + keynum;

		memcached.get(keyname, null);
	}

	public long doTransactionGets(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		return memcached.gets(Config.getConfig().key_prefix + keynum);
	}

	public void doTransactionIncr(Memcached memcached) {

	}

	public void doTransactionPrepend(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		memcached.prepend(key, 0, "prepended_string");
	}

	public void doTransactionReplace(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.replace(key, value);
	}

	public void doTransactionSet(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.set(keyname, value);
	}

	public void doTransactionUpdate(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.update(keyname, value);
	}
}
