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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;

import com.alibaba.flink.connectors.common.exception.ErrorUtils;
import com.alibaba.flink.connectors.common.reader.AbstractPartitionNumsListener;
import com.alibaba.flink.connectors.common.reader.Interruptible;
import com.alibaba.flink.connectors.common.reader.RecordReader;
import com.alibaba.flink.connectors.common.util.RetryUtils;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sls record reader.
 */
public class SlsRecordReader extends AbstractPartitionNumsListener implements RecordReader<List<LogGroupData>, String>,
		Interruptible {
	private static final Logger LOG = LoggerFactory.getLogger(SlsRecordReader.class);

	protected String endPoint = null;
	protected String accessKeyId = null;
	protected String accessKeySecret = null;
	protected String project = null;
	protected String logStore = null;
	protected int startInSec;
	protected int stopInSec;
	protected int maxRetryTime = 3;
	private int batchGetSize = 10;
	private int shardId = -1;
	private String lastSuccessfulCursor;
	private long lastMonotonyIncreaseProgress = -1;
	private String nextBeginCursor;
	private String stopCursor;
	private String consumerGroup = null;
	SlsClientProvider slsClientProvider = null;
	private long currentWatermark;
	private int lastSuccessMessageTimestamp;
	private volatile boolean interrupted = false;
	private transient List<LogGroupData>  rawLogGroupDatas;

	private Configuration properties;
	private long lastLogPrintTime = 0L;
	private long dataFetchedDelay = 0;

	boolean genFetchTask = true;
	long mLastFetchRawSize = Long.MAX_VALUE;
	long mLastFetchCount = Long.MAX_VALUE;
	long mLastFetchTime = Long.MIN_VALUE;
	private boolean directMode = false;
	private transient Shard shard;
	private boolean isReadOnlyShard = false;
	private String endCursor;
	private long lastPartitionChangedTime = 0L;

	public SlsRecordReader(
			String endPoint,
			String accessKeyId,
			String accessKeySecret,
			String project,
			String logStore,
			int startInSec,
			int stopInSec,
			int maxRetryTime,
			int batchGetSize,
			List<Shard> initShardList,
			Configuration properties,
			String consumerGroup) {
		this.endPoint = endPoint;
		this.accessKeyId = accessKeyId;
		this.accessKeySecret = accessKeySecret;
		this.project = project;
		this.logStore = logStore;
		this.startInSec = startInSec;
		this.stopInSec = stopInSec;
		this.maxRetryTime = maxRetryTime;
		this.batchGetSize = batchGetSize;
		setInitPartitionCount(null == initShardList ? 0 : initShardList.size());
		this.properties = properties;
		this.consumerGroup = consumerGroup;
	}

	public SlsRecordReader(
			String endPoint,
			Configuration properties,
			String project,
			String logStore,
			int startInSec,
			int stopInSec,
			int maxRetryTime,
			int batchGetSize,
			List<Shard> initShardList,
			String consumerGroup) {
		this.endPoint = endPoint;
		this.project = project;
		this.logStore = logStore;
		this.startInSec = startInSec;
		this.stopInSec = stopInSec;
		this.maxRetryTime = maxRetryTime;
		this.batchGetSize = batchGetSize;
		setInitPartitionCount(null == initShardList ? 0 : initShardList.size());
		this.properties = properties;
		this.consumerGroup = consumerGroup;
	}

	public List<Shard> getShardsList() throws LogException {
		slsClientProvider = getSlsClientProvider();
		return slsClientProvider.getClient().ListShard(project, logStore).GetShards();
	}

	@Override
	public int getPartitionsNums() {
		try {
			int count = getShardsList().size();
			LOG.info("Get {} shards from SLS", count);
			return count;
		} catch (LogException e) {
			LOG.info("Error fetching shard list", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getReaderName() {
		return "SlsRecordReader-" + project + "-" + logStore + " endPoint:" + endPoint;
	}

	SlsClientProvider getSlsClientProvider(){
		if (null == slsClientProvider) {
			if (null != accessKeyId && null != accessKeySecret && !accessKeyId.isEmpty() &&
				!accessKeySecret.isEmpty()) {
				slsClientProvider = new SlsClientProvider(
						endPoint,
						accessKeyId,
						accessKeySecret,
						consumerGroup,
						directMode);
			} else {
				slsClientProvider = new SlsClientProvider(
						endPoint,
						properties,
						consumerGroup,
						directMode);
			}
		}
		return slsClientProvider;
	}

	@Override
	public void open(
			InputSplit split, RuntimeContext context) throws IOException {
		SlsInputSplit slsInputSplit = (SlsInputSplit) split;
		LOG.info(String.format("open project[%s] logStore[%s],consumer[%s]-%s  startTime[%d)", project, logStore,
							accessKeyId,
							slsInputSplit.toString(), startInSec));

		int curRetry = 0;
		while (curRetry++ < maxRetryTime) {
			try {
				List<Shard> shardsList = getShardsList();
				if (initPartitionCount != shardsList.size()){
					ErrorUtils.throwException(
							String.format("Source {%s} partitions number has changed from {%s} to {%s} \n " +
											"Wait the failover finish, blink is trying to recovery from " +
											"source partition change", getReaderName(),
									initPartitionCount, shardsList.size()));
				}
				this.shardId = split.getSplitNumber();
				for (Shard shard: shardsList) {
					if (shard.GetShardId() == this.shardId){
						this.shard = shard;
						break;
					}
				}
				if (shard.getStatus().equalsIgnoreCase("readonly")) {
					LOG.info("ShardId " + shard.GetShardId() + " status:readOnly");
					isReadOnlyShard = true;
					this.endCursor = getSlsClientProvider().getClient().GetCursor(project, logStore, shardId, Consts
							.CursorMode.END).GetCursor();
				} else {
					LOG.info("ShardId " + shard.GetShardId() + " status:readwrite");
					isReadOnlyShard = false;
				}
				this.nextBeginCursor = getSlsClientProvider().getClient()
															.GetCursor(project, logStore, shardId, startInSec)
															.GetCursor();
				if (stopInSec == Integer.MAX_VALUE) {
					this.stopCursor = null;

				} else {
					this.stopCursor = getSlsClientProvider().getClient()
							.GetCursor(project, logStore, shardId, stopInSec).GetCursor();
				}

				if (consumerGroup == null) {
					LOG.info(String.format(
							"Open method get init cursor, " +
							"project[%s]-logStore[%s]-shardId[%d]-startInSec[%d]-Cursor[%s]",
							project,
							logStore,
							shardId,
							startInSec,
							nextBeginCursor));
				} else {
					LOG.info(String.format(
							"Open method get init cursor, " +
							"project[%s]-logStore[%s]-shardId[%d]-startInSec[%d]-Cursor[%s]-ConsumerGroup[%s]",
							project,
							logStore,
							shardId,
							startInSec,
							nextBeginCursor,
							consumerGroup));
				}
				break;
			} catch (LogException e) {
				LOG.error("Error in get shard list", e);
				// refresh sts account
				getSlsClientProvider().getClient(true, true);
				if (curRetry == maxRetryTime) {
					ErrorUtils.throwException(
							e.getMessage());
				}
				try {
					Thread.sleep(curRetry * 500);
				} catch (Exception e1) {

				}
			}
		}
		initPartitionNumsListener();
	}

	@Override
	public boolean next() throws IOException, InterruptedException {

		List<LogGroupData> datas = new ArrayList<>();
		int currRetryTime = 0;
		while (datas.size() == 0 && (stopCursor == null || !stopCursor.equals(nextBeginCursor))) {
			if (isPartitionChanged()){
				ErrorUtils.throwException(
						String.format("Source {%s} partitions number has changed from {%s} to {%s} \n " +
										"Wait the failover finish, blink is trying to recovery from " +
										"source partition change", getReaderName(),
								initPartitionCount, getPartitionsNums()));
			}
			if (interrupted) {
				return false;
			}
			if (isReadOnlyShard  && nextBeginCursor.equals(endCursor)){
				LOG.info(String.format("CurrentRecordRead reached end, " +
									"project[%s]-logStore[%s]-shardId[%d]-progress[%d]-delay[%d]-Cursor[%s]-" +
									"nextCursor[%s]-EndCursor[%s]", project,
									logStore, shardId, lastSuccessMessageTimestamp,
									dataFetchedDelay,
									lastSuccessfulCursor,
									nextBeginCursor,
									endCursor));
				return false;
			}
			// 退火算法减轻对服务端的压力
			genFetchTask = true;
			if (mLastFetchRawSize < 1024 * 1024 && mLastFetchCount < batchGetSize) {
				genFetchTask = (System.currentTimeMillis() - mLastFetchTime > 500);
			} else if (mLastFetchRawSize < 2 * 1024 * 1024 && mLastFetchCount < batchGetSize) {
				genFetchTask = (System.currentTimeMillis() - mLastFetchTime > 200);
			} else if (mLastFetchRawSize < 4 * 1024 * 1024 && mLastFetchCount < batchGetSize) {
				genFetchTask = (System.currentTimeMillis() - mLastFetchTime > 50);
			}
			if (genFetchTask) {
				try {
					BatchGetLogResponse batchGetLogResponse = null;

					batchGetLogResponse = getSlsClientProvider().getClient().BatchGetLog(
							project,
							logStore,
							shardId,
							batchGetSize,
							nextBeginCursor,
							stopCursor);
					mLastFetchRawSize = batchGetLogResponse.GetRawSize();
					mLastFetchCount = batchGetLogResponse.GetCount();
					mLastFetchTime = System.currentTimeMillis();

					if (batchGetLogResponse.GetCount() > 0) {

						lastMonotonyIncreaseProgress = lastSuccessMessageTimestamp;
						lastSuccessfulCursor = nextBeginCursor;
						lastSuccessMessageTimestamp = getSlsClientProvider().getClient()
																.GetCursorTime(project, logStore,
																				shardId,
																				batchGetLogResponse.GetNextCursor())
																.GetCursorTime();
						currentWatermark = lastSuccessMessageTimestamp * 1000L;
						dataFetchedDelay = System.currentTimeMillis() - currentWatermark;
						datas.addAll(batchGetLogResponse.GetLogGroups());
					}

					nextBeginCursor = batchGetLogResponse.GetNextCursor();
				} catch (LogException e) {
					// refresh sts account
					getSlsClientProvider().getClient(true, true);
					currRetryTime++;
					if (currRetryTime <= maxRetryTime) {
						Thread.sleep(currRetryTime * 1000);
					} else {
						throw new RuntimeException("ERROR in next method :"
								+ "ErrorCode " + e.GetErrorCode()
								+ "ErrorMessage " + e.GetErrorMessage()
								+ "RequestId " + e.GetRequestId());
					}
				}
			} else {
				try {
					Thread.sleep(50);
				} catch (Exception e){
					//ignore
				}
			}
		}

		// reach end.
		if (datas.isEmpty()) {
			return false;
		}

		// 每分钟打印一条delay日志
		if (System.currentTimeMillis() - lastLogPrintTime >= 60000) {
			if (null != consumerGroup) {
				// 更新服务端的消费进度
				try {
					getSlsClientProvider().getClient().UpdateCheckPoint(project, logStore, consumerGroup, shardId,
																		lastSuccessfulCursor);
				} catch (Exception e){
					LOG.error("Update CheckPoint Error and Ignore it ", e);
					//ignore this exception
				}
			}
			lastLogPrintTime = System.currentTimeMillis();
			LOG.info(String.format("Next method get current cursor, " +
								"project[%s]-logStore[%s]-shardId[%d]-progress[%d]-delay[%d]-Cursor[%s]-" +
								"nextCursor[%s]-EndCursor[%s]", project,
								logStore, shardId, lastSuccessMessageTimestamp,
								System.currentTimeMillis() - lastSuccessMessageTimestamp,
								lastSuccessfulCursor,
								nextBeginCursor,
								stopCursor));
		}
		rawLogGroupDatas = datas;
		return true;
	}

	@Override
	protected void partitionNumsChangeListener(int newPartitionsCount, int initPartitionCount) {
		if (newPartitionsCount > initPartitionCount) {
			LOG.warn("shard count changed from {} to {}", initPartitionCount, newPartitionsCount);
			triggerPartitionNumFailOver();
		} else if (newPartitionsCount < initPartitionCount){
			LOG.warn("shard count changed from {} to {}", initPartitionCount, newPartitionsCount);
			if (lastPartitionChangedTime == 0L){
				lastPartitionChangedTime = System.currentTimeMillis();
			} else if (System.currentTimeMillis() - lastPartitionChangedTime > 5 * 60 * 1000){
				triggerPartitionNumFailOver();
			}
		} else {
			lastPartitionChangedTime = 0L;
		}
	}

	@Override
	public List<LogGroupData> getMessage() {
		return rawLogGroupDatas;
	}

	@Override
	public void close() throws IOException {
		destroyPartitionNumsListener();
	}

	@Override
	public void seek(String s) throws IOException {
		// 0 means the start of
		if (s.equalsIgnoreCase(SlsSourceFunction.NEW_SLS_START_FLAG)) {
			try {
				this.nextBeginCursor = RetryUtils.executeWithRetry(
						() -> getSlsClientProvider().getClient().GetCursor(project, logStore, shardId, Consts.CursorMode.BEGIN).GetCursor(),
						maxRetryTime, 10000, false);
			} catch (Exception e){
				throw ErrorUtils.getException(e.getMessage());
			}
		} else {
			this.nextBeginCursor = s;
		}
	}

	@Override
	public String getProgress() throws IOException {
		return nextBeginCursor;
	}

	@Override
	public long getDelay() {
		return currentWatermark;
	}

	@Override
	public long getFetchedDelay() {
		return dataFetchedDelay;
	}

	@Override
	public boolean isHeartBeat() {
		return false;
	}

	@Override
	public long getWatermark() {
		return currentWatermark;
	}

	@Override
	public void interrupt() {
		interrupted = true;
	}

	public SlsRecordReader setDirectMode(boolean directMode) {
		this.directMode = directMode;
		return this;
	}
}
