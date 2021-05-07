/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.sls.datastream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;

import com.alibaba.flink.connectors.common.reader.RecordReader;
import com.alibaba.flink.connectors.common.source.AbstractDynamicParallelSource;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * SlsSourceFunction.
 */
public class SlsSourceFunction extends AbstractDynamicParallelSource<List<LogGroupData>, String> {
	private static final long serialVersionUID = 6289824294498842746L;
	private static final Logger LOG = LoggerFactory.getLogger(SlsSourceFunction.class);

	public static final String NEW_SLS_START_FLAG = "new_sls_start_flag";

	protected String endPoint = null;
	protected String accessKeyId = null;
	protected String accessKeySecret = null;
	protected String project = null;
	protected String logStore = null;
	private String consumerGroup;
	private Configuration properties;
	protected int maxRetryTime = 3;
	private int batchGetSize = 10;
	private int startInSec = 0;
	private int stopInSec = Integer.MAX_VALUE;
	private boolean directMode = false;
	private List<Shard> initShardList = new ArrayList<>();

	private transient SlsClientProxy clientProxy = null;

	public SlsSourceFunction(
			String endPoint,
			String accessKeyId,
			String accessKeySecret,
			String project,
			String logStore,
			long startInMs,
			long stopInMs,
			int maxRetryTime,
			int batchGetSize,
			Configuration properties,
			String consumerGroup) throws Exception {
		this.endPoint = endPoint;
		this.accessKeyId = accessKeyId;
		this.accessKeySecret = accessKeySecret;
		this.project = project;
		this.logStore = logStore;
		this.properties = properties;
		this.consumerGroup = consumerGroup;
		this.maxRetryTime = maxRetryTime;
		this.batchGetSize = batchGetSize;
		this.startInSec = (int) (startInMs / 1000);
		this.stopInSec =
				stopInMs / 1000 > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (stopInMs / 1000);
		initShardList();
		init();
	}

	public SlsSourceFunction(
			String endPoint,
			Configuration properties,
			String project,
			String logStore,
			long startInMs,
			long stopInMs,
			int maxRetryTime,
			int batchGetSize,
			String consumerGroup
	) throws Exception {
		this.endPoint = endPoint;
		this.project = project;
		this.logStore = logStore;
		this.properties = properties;
		this.consumerGroup = consumerGroup;
		this.maxRetryTime = maxRetryTime;
		this.batchGetSize = batchGetSize;
		this.startInSec = (int) (startInMs / 1000);
		this.stopInSec =
				stopInMs / 1000 > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (stopInMs / 1000);
		initShardList();
		init();
	}

	private SlsClientProxy getClientProxy() {
		if (clientProxy == null) {
			this.clientProxy = new SlsClientProxy(endPoint, accessKeyId, accessKeySecret, project, logStore, consumerGroup, properties);
			this.clientProxy.setDirectMode(directMode);
		}
		return clientProxy;
	}

	private void init() {
		/**
		 * consumerGroup一旦创建后会保存在服务端。
		 * 首先创建consumerGroup，如果之前已经创建过则比较conuserGroup的信息是否一致，
		 */
		getClientProxy().ensureConsumerGroupCreated();
	}

	@Override
	public RecordReader<List<LogGroupData>, String> createReader(Configuration config) throws IOException {
		return new SlsRecordReader(
				endPoint,
				accessKeyId,
				project,
				logStore,
				startInSec,
				stopInSec,
				maxRetryTime,
				batchGetSize,
				initShardList,
				consumerGroup,
				getClientProxy());
	}

	@Override
	public InputSplit[] createInputSplitsForCurrentSubTask(
			int numberOfParallelSubTasks, int indexOfThisSubTask) throws IOException {
		List<Shard> subscribedPartitions =
				modAssign(numberOfParallelSubTasks, indexOfThisSubTask);

		SlsInputSplit[] inputSplits = new SlsInputSplit[subscribedPartitions.size()];
		int i = 0;
		for (Shard shard : subscribedPartitions) {
			inputSplits[i++] = new SlsInputSplit(shard.GetShardId());
		}
		return inputSplits;
	}

	@Override
	public List<Tuple2<InputSplit, String>> reAssignInputSplitsForCurrentSubTask(
			int numberOfParallelSubTasks, int indexOfThisSubTask, List<InnerProgress<String>> allSplitsInState)
			throws IOException {
		List<Tuple2<InputSplit, String>> initialProgess = new ArrayList<>();
		List<Shard> subscribedPartitions = modAssign(numberOfParallelSubTasks, indexOfThisSubTask);
		for (Shard shard : subscribedPartitions) {
			boolean existBefore = false;
			for (InnerProgress<String> progress: allSplitsInState) {
				if (shard.GetShardId() == progress.getInputSplit().getSplitNumber()){
					initialProgess.add(new Tuple2<>(progress.getInputSplit(), progress.getCursor()));
					existBefore = true;
					break;
				}
			}
			if (!existBefore) {
				// 新增加的shardId 标识0为shard的开头
				initialProgess.add(Tuple2.of(new SlsInputSplit(shard.GetShardId()), NEW_SLS_START_FLAG));
			}
		}

		return initialProgess;
	}

	@Override
	public List<String> getPartitionList() throws Exception {
		List<String> partitions = new ArrayList<>();
		List<Shard> shards = getClientProxy().listShards();
		for (Shard shard : shards) {
			partitions.add("" + shard.GetShardId());
		}
		return partitions;
	}

	@Override
	public void open(Configuration config) throws IOException {
		initShardList();
		super.open(config);
		LOG.info("Init source succ.");
	}

	@Override
	public void close() throws IOException {
		super.close();
	}

	private void initShardList() {
		if (null != initShardList) {
			try {
				initShardList = getClientProxy().listShards();
				Collections.sort(initShardList, new Comparator<Shard>() {
					@Override
					public int compare(Shard o1, Shard o2) {
						return o1.GetShardId() - o2.GetShardId();
					}
				});
			} catch (Exception e){
				throw new RuntimeException("", e);
			}
		}
	}

	public SlsSourceFunction setDirectMode(boolean directMode) {
		if (clientProxy != null) {
			clientProxy.setDirectMode(directMode);
		}
		this.directMode = directMode;
		return this;
	}

	private List<Shard> modAssign(
			int consumerCount,
			int consumerIndex) {
		List<Shard> assignedShards = new LinkedList<>();

		for (Shard shard: initShardList) {
			if (shard.GetShardId() % consumerCount == consumerIndex) {
				assignedShards.add(shard);
			}
		}
		return assignedShards;
	}

	@Override
	public String toString() {
		return String.format("Sls Source from %s.%s", project, logStore);
	}
}
