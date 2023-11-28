/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Partitioner selects the target channel based on the key group index.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class KeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	private static final long serialVersionUID = 1L;

	private final KeySelector<T, K> keySelector;

	private int maxParallelism;
	//add for megaphone
	private HashMap<Integer, Integer> oldRouteTable = new HashMap<>();//key:键组索引, value:目标task
	private HashMap<Integer, Integer> megaphoneRouteTable = new HashMap<>();//key:键组索引, value:目标task
	private long controlWatermark = Long.MAX_VALUE;//时间戳大于control时采取megaphoneRouteTable，否则使用oldRouteTable


	public KeyGroupStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
	}

	@Override
	public void setup(int numberOfChannels) {
		super.setup(numberOfChannels);
		initOldRouteTable(maxParallelism, numberOfChannels);
	}

	private void initOldRouteTable(int maxParallelism, int numberOfChannels){
		for(int i=0;i<maxParallelism;++i){
			oldRouteTable.put(i, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, numberOfChannels, i));
		}
		//System.out.println(oldRouteTable);
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		synchronized (this) {
			K key;
			try {
				key = keySelector.getKey(record.getInstance().getValue());
			} catch (Exception e) {
				throw new RuntimeException(
					"Could not extract key from " + record.getInstance().getValue(), e);
			}
			int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
			//System.out.println("key:"+key+" , keyGroup:"+keyGroupIndex);
			if (record.getInstance().getTimestamp() >= controlWatermark) {
				return megaphoneRouteTable.containsKey(keyGroupIndex) ?
					megaphoneRouteTable.get(keyGroupIndex) : oldRouteTable.get(keyGroupIndex);
			}
//			System.out.println("key:"+key+" , keyGroupIndex:"+keyGroupIndex+" , oldRouteTable:"+
//				oldRouteTable.get(keyGroupIndex)+" , nativeRouteTable:"+
//				KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels));
			return oldRouteTable.get(keyGroupIndex);
			//return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);
		}
	}

	@Override
	public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
		return SubtaskStateMapper.RANGE;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	@Override
	public void configure(int maxParallelism) {
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		final KeyGroupStreamPartitioner<?, ?> that = (KeyGroupStreamPartitioner<?, ?>) o;
		return maxParallelism == that.maxParallelism &&
			keySelector.equals(that.keySelector);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), keySelector, maxParallelism);
	}

	public void updateControl(long controlWatermark, int keyGroupIndex, int targetIndex){
		synchronized (this){
			//System.out.println("before update:");
			//System.out.println("oldRouteTable:"+oldRouteTable);
			//System.out.println("megaphoneRouteTable:"+megaphoneRouteTable);
			this.controlWatermark = controlWatermark;
			oldRouteTable.putAll(megaphoneRouteTable);
			megaphoneRouteTable.clear();
			megaphoneRouteTable.put(keyGroupIndex, targetIndex);
			//System.out.println("after update:");
			//System.out.println("oldRouteTable:"+oldRouteTable);
			//System.out.println("megaphoneRouteTable:"+megaphoneRouteTable);
		}
	}
}
