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

package com.alibaba.flink.connectors.cloudhbase.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.StringUtils;

import com.alibaba.flink.connectors.common.MetricUtils;
import com.alibaba.flink.connectors.common.exception.ErrorUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A sink function for Aliyun CloudHBase. The main features are as follows:
 * 1. only support synchronous flush
 * 2. buffer the records to dismiss the duplicated row key
 * 3. batch records to hbase when flushing
 * 4. flush to hbase periodically no matter whether the buffer is filled up
 */
public class CloudHBaseSinkFunction<RECORD> extends RichSinkFunction<RECORD> implements ListCheckpointed<byte[]>  {

	private static final Logger LOG = LoggerFactory.getLogger(CloudHBaseSinkFunction.class);

	public static final int DEFAULT_BUFFER_SIZE = 5000;
	public static final int DEFAULT_MAX_RETRY_NUMBER = 3;
	public static final int DEFAULT_BATCH_SIZE = 100;
	public static final long DEFAULT_FLUSH_INTERVAL = 2000;

	private static final long RETRY_INTERVAL = 100;
	private static final long RETRY_TIMEOUT = 30 * 60 * 1000;

	private String zkQuorum;
	private String zkZNodeParent;
	private String tableName;
	private int maxRetryNumber;

	private int bufferSize;
	private int batchSize;
	private long flushInterval;
	private CloudHBaseRecordResolver<RECORD> recordResolver;

	private transient Connection hbaseConn;

	private transient Map<String, RECORD> writeBuffer;
	private transient long lastFlushTime;

	private transient ScheduledThreadPoolExecutor flusher;
	private transient volatile Exception flushException;
	private transient volatile boolean flushError;

	private transient Meter outTps;
	private transient Meter outBps;
	private transient MetricUtils.LatencyGauge latencyGauge;

	public CloudHBaseSinkFunction(
			String zkQuorum,
			String tableName,
			CloudHBaseRecordResolver<RECORD> recordResolver) {
		this(zkQuorum,
				null,
				tableName,
				DEFAULT_BUFFER_SIZE,
				DEFAULT_BATCH_SIZE,
				DEFAULT_FLUSH_INTERVAL,
				DEFAULT_MAX_RETRY_NUMBER,
				recordResolver);
	}

	public CloudHBaseSinkFunction(
			String zkQuorum,
			String zkZNodeParent,
			String tableName,
			int bufferSize,
			int batchSize,
			long flushInterval,
			int maxRetryNumber,
			CloudHBaseRecordResolver<RECORD> recordResolver) {
		this.zkQuorum = zkQuorum;
		this.zkZNodeParent = zkZNodeParent;
		this.tableName = tableName;
		this.bufferSize = bufferSize;
		this.batchSize = batchSize;
		this.flushInterval = flushInterval;
		this.maxRetryNumber = maxRetryNumber;
		this.recordResolver = recordResolver;
	}

	public String getZkQuorum() {
		return zkQuorum;
	}

	public String getZkZNodeParent() {
		return zkZNodeParent;
	}

	public String getTableName() {
		return tableName;
	}

	public int getMaxRetryNumber() {
		return maxRetryNumber;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public long getFlushInterval() {
		return flushInterval;
	}

	public CloudHBaseRecordResolver getCloudHBaserecordResolver() {
		return recordResolver;
	}

	@Override
	public void open(Configuration config) throws IOException {
		this.recordResolver.open(config);

		// create hbase connection
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
		conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
		if (!StringUtils.isNullOrWhitespaceOnly(zkZNodeParent)) {
			conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkZNodeParent);
		}
		conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 30000);
		conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, maxRetryNumber);
		conf.set(HConstants.RPC_CODEC_CONF_KEY, "");
		this.hbaseConn = ConnectionFactory.createConnection(conf);

		this.writeBuffer = new HashMap<>();
		this.lastFlushTime = 0;
		this.flushError = false;

		startFlusher();

		this.outTps = MetricUtils.registerOutTps(getRuntimeContext());
		this.outBps = MetricUtils.registerOutBps(getRuntimeContext(), "cloudhbase");
		this.latencyGauge = MetricUtils.registerOutLatency(getRuntimeContext());

		LOG.info("CloudHBase is opened");
	}

	private void startFlusher() {
		this.flusher = new ScheduledThreadPoolExecutor(1,
				new DispatcherThreadFactory(Thread.currentThread().getThreadGroup(), "CloudHBase Flusher"));
		this.flusher.setRemoveOnCancelPolicy(true);
		this.flusher.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.flusher.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

		this.flusher.scheduleAtFixedRate(() -> {
			if (System.currentTimeMillis() - lastFlushTime >= flushInterval) {
				flush(false);
			}
		}, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
	}

	@Override
	public void invoke(RECORD record) throws IOException {
		if (flushError) {
			throw new RuntimeException(flushException);
		}

		String rowKey = recordResolver.getRowKey(record);
		synchronized (writeBuffer) {
			writeBuffer.put(rowKey, record);
		}

		if (writeBuffer.size() >= bufferSize) {
			flush(false);
		}
	}

	@Override
	public void close() throws IOException {
		if (flusher != null) {
			flusher.shutdownNow();
			flusher = null;
		}

		try {
			flush(true);
		} catch (Exception e) {
			LOG.error("flush failed when closing", e);
		}

		if (hbaseConn != null) {
			try {
				hbaseConn.close();
				hbaseConn = null;
			} catch (Exception e) {
				LOG.error("close hbase connection failed", e);
			}
		}

		this.recordResolver.close();

		LOG.info("CloudHBase is closed");
	}

	private void flush(boolean force) {
		synchronized (writeBuffer) {
			if (flushError) {
				ErrorUtils.throwException(String.format("An error has occurred before, %s", flushException));
			}

			long startTime = System.currentTimeMillis();
			// check whether the buffer should be flushed
			if (!force && writeBuffer.size() < bufferSize && startTime - lastFlushTime < flushInterval) {
				return;
			}

			Table table = null;
			try {
				long writeByteSize = 0L;
				table = this.hbaseConn.getTable(TableName.valueOf(tableName));

				List<Mutation> mutations = new ArrayList<>(writeBuffer.size());
				for (RECORD record : writeBuffer.values()) {
					Mutation mutation = recordResolver.getMutation(record);
					if (mutation != null) {
						mutations.add(mutation);
						writeByteSize += mutation.size();
					}
				}

				int startIndex = 0;
				int endIndex = batchSize;
				while (startIndex < mutations.size()) {
					Object[] res;
					if (endIndex < mutations.size()) {
						res = new Object[batchSize];
						table.batch(mutations.subList(startIndex, endIndex), res);
					} else {
						res = new Object[mutations.size() - startIndex];
						table.batch(mutations.subList(startIndex, mutations.size()), res);
					}
					for (Object o : res) {
						if (o == null) {
							ErrorUtils.throwException(String.format("table %s batch operate error", tableName));
						}
					}
					startIndex = endIndex;
					endIndex = endIndex + batchSize;
				}

				// report metrics
				if (latencyGauge != null) {
					long endTime = System.currentTimeMillis();
					latencyGauge.report(endTime - startTime, writeBuffer.size());
				}
				if (outTps != null) {
					outTps.markEvent(writeBuffer.size());
				}
				if (outBps != null) {
					outBps.markEvent(writeByteSize);
				}
			} catch (Exception e) {
				LOG.error("table {} sync failed", tableName, e);
				flushError = true;
				flushException = e;
				throw new RuntimeException(String.format("table %s sync failed, %s", tableName, e.getMessage()));
			} finally {
				if (table != null) {
					try {
						table.close();
					} catch (Exception e) {
						LOG.warn("close table {} failed", tableName, e);
					}
				}
			}
			lastFlushTime = System.currentTimeMillis();
			writeBuffer.clear();
		}
	}

	@Override
	public List<byte[]> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		long startTime = System.currentTimeMillis();
		while (true) {
			try {
				flush(true);
				break;
			} catch (Exception e) {
				LOG.error("table {} sync failed in snapshotState", tableName, e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					// ignore
				}
			}

			long duration = System.currentTimeMillis() - startTime;
			if (duration > RETRY_TIMEOUT) {
				throw new IOException(String.format("Retry time exceeds timeout: %s, %s", duration, RETRY_TIMEOUT));
			}
		}
		return null;
	}

	@Override
	public void restoreState(List<byte[]> state) {
	}
}
