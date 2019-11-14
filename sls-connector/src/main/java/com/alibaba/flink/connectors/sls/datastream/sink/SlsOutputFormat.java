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

package com.alibaba.flink.connectors.sls.datastream.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.flink.connectors.common.MetricUtils;
import com.alibaba.flink.connectors.common.sink.HasRetryTimeout;
import com.alibaba.flink.connectors.common.sink.Syncable;
import com.alibaba.flink.sls.shaded.com.google.common.util.concurrent.FutureCallback;
import com.alibaba.flink.sls.shaded.com.google.common.util.concurrent.Futures;
import com.alibaba.flink.sls.shaded.com.google.common.util.concurrent.ListenableFuture;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.aliyun.log.producer.errors.ResultFailedException;
import com.aliyun.openservices.log.common.LogItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sls output format which write record to sls which given serialization schema.
 * @param <T>  type of input record.
 */
public class SlsOutputFormat<T> extends RichOutputFormat<T> implements Syncable,
		HasRetryTimeout {
	private static final long serialVersionUID = -8829623574993898905L;

	private static final Logger LOG = LoggerFactory.getLogger(SlsOutputFormat.class);
	private boolean failOnError = true;
	private int maxRetryTime = 3;

	/** Properties to settings. */
	private String endPoint;
	private String projectName;
	private String logstore;
	private String accessKeyId;
	private String accessKey;

	private Configuration properties;

	// --------------------------- Runtime fields ---------------------------
	private int retryTimeoutMS = 1000 * 300;
	private LogProducerProvider logProducerProvider;
	private boolean useSts = false;

	private Meter outTps;
	private MetricUtils.LatencyGauge latencyGauge;

	private int flushInterval = 2000;
	private AtomicLong numSent = new AtomicLong(0);
	private AtomicLong numCommitted = new AtomicLong(0);

	private transient SendFutureCallback sendFutureCallback;
	private transient ExecutorService executor;
	private transient RuntimeException callBackException = null;
	private String timeZone;

	private SlsRecordResolver<T> serializationSchema;

	public SlsOutputFormat(
			Configuration properties,
			String endPoint,
			String accessKeyId,
			String accessKey,
			String projectName,
			String logstoreName,
			SlsRecordResolver<T> serializationSchema) {

		this.properties = properties;
		this.endPoint = endPoint;
		this.projectName = projectName;
		this.logstore = logstoreName;
		this.accessKeyId = accessKeyId;
		this.accessKey = accessKey;
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
	}

	public SlsOutputFormat(
			Configuration properties,
			String endPoint,
			String projectName,
			String logstoreName,
			SlsRecordResolver<T> serializationSchema) {

		this.endPoint = endPoint;
		this.properties = properties;
		this.projectName = projectName;
		this.logstore = logstoreName;
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
	}

	public SlsOutputFormat setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
		return this;
	}

	public SlsOutputFormat setTimeZone(String timeZone) {
		this.timeZone = timeZone;
		return this;
	}

	public SlsOutputFormat setFlushInterval(int flushInterval) {
		this.flushInterval = flushInterval;
		return this;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		sendFutureCallback = new SendFutureCallback();
		executor = Executors.newSingleThreadExecutor();

		if (!StringUtils.isNullOrWhitespaceOnly(accessKey) && !StringUtils.isNullOrWhitespaceOnly(accessKeyId)) {
			logProducerProvider = new LogProducerProvider(
														projectName,
														endPoint,
														properties,
														accessKeyId,
														accessKey,
														maxRetryTime,
														flushInterval);
			useSts = false;
		} else {
			logProducerProvider = new LogProducerProvider(projectName, endPoint, properties, maxRetryTime, flushInterval);
			useSts = true;
		}

		outTps = MetricUtils.registerOutTps(getRuntimeContext());
		latencyGauge = MetricUtils.registerOutLatency(getRuntimeContext());
	}

	@Override
	public void writeRecord(T row) throws IOException {

		if (null != row) {
			long start = System.currentTimeMillis();
			List<LogItem> tmpLogGroup = new ArrayList<>(1);
			tmpLogGroup.add(serializationSchema.getLogItem(row));

			/** calc the partition key, if not set, use null as random shard **/
			String partitionKey = serializationSchema.getPartitionKey(row);
			String topic = serializationSchema.getTopic(row);
			String source = serializationSchema.getSource(row);
			try {
				ListenableFuture<Result> future = logProducerProvider.getClient().send(
						this.projectName, this.logstore, topic, source, partitionKey, tmpLogGroup);
				Futures.addCallback(future, sendFutureCallback, executor);
				numSent.incrementAndGet();
			} catch (InterruptedException | ProducerException e) {
				callBackException = new RuntimeException(e);
			}

			if (null != callBackException) {
				LOG.warn("Fail in write to SLS", callBackException);
				if (failOnError) {
					throw callBackException;
				}
				callBackException = null;
			}

			// report metrics
			long end = System.currentTimeMillis();
			latencyGauge.report(end - start, 1);
			outTps.markEvent();
		}

	}

	@Override
	public void sync() throws IOException {

		while (numSent.get() != numCommitted.get()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				throw new IOException("Fail to write to sls", e);
			}
		}

		if (callBackException != null) {
			LOG.error("Fail to write to sls", callBackException);
			if (failOnError) {
				throw new IOException("Fail to write to sls", callBackException);
			}
			callBackException = null;
		}
	}

	@Override
	public void close() throws IOException {

		LOG.info("closing producer");
		try {
			sync();
		} finally {
			if (logProducerProvider != null) {
				logProducerProvider.closeClient();
			}
		}
		LOG.info("Flushing done. Closing the producer instance.");
	}

	public void setMaxRetryTime(int maxRetryTime) {
		this.maxRetryTime = maxRetryTime;
	}

	/**
	 * Builder for SlsOutputFormat.
	 */
	public static class Builder<T> {
		private String endPoint;
		private String projectName;
		private String logstore;
		private String accessKeyId;
		private String accessKey;
		private Configuration properties;
		private int flushInterval = 2000;
		private int maxRetryTimes = 3;
		private boolean failOnError = true;
		private SlsRecordResolver<T> serializationSchema;

		public Builder setSerializationSchema(SlsRecordResolver<T> serializationSchema) {
			this.serializationSchema = serializationSchema;
			return this;
		}

		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setFailOnError(boolean failOnError) {
			this.failOnError = failOnError;
			return this;
		}

		public Builder setFlushInterval(int flushInterval) {
			this.flushInterval = flushInterval;
			return this;
		}

		void checkEmptyString(String target, String errorMsg) {
			Preconditions.checkArgument(
					!StringUtils.isNullOrWhitespaceOnly(target), errorMsg);
		}

		void checkEmptyList(List<String> list, String errorMsg) {
			Preconditions.checkArgument(
					list != null && !list.isEmpty(),
					errorMsg);
		}

		public Builder setProjectName(String projectName) {
			checkEmptyString(projectName, "projectName is empty!");
			this.projectName = projectName;
			return this;
		}

		public Builder setLogstore(String logstore) {
			checkEmptyString(logstore, "logstore is empty!");
			this.logstore = logstore;
			return this;
		}

		public String getLogstore() {
			return  this.logstore;
		}

		public Builder setEndPoint(String endPoint) {
			checkEmptyString(endPoint, "endPoint is empty!");
			this.endPoint = endPoint;
			return this;
		}

		public Builder setAccessKeyId(String accessId) {
			this.accessKeyId = accessId;
			return this;
		}

		public Builder setAccessKey(String accessKey) {
			this.accessKey = accessKey;
			return this;
		}

		public void setProperties(Configuration properties) {
			this.properties = properties;
		}

		public SlsOutputFormat<T> build() {
			SlsOutputFormat outputFormat = null;
			if (!StringUtils.isNullOrWhitespaceOnly(accessKeyId) && !StringUtils.isNullOrWhitespaceOnly(accessKey)) {
				outputFormat = new SlsOutputFormat(properties, endPoint,
												accessKeyId, accessKey, projectName, logstore, serializationSchema);
			} else {
				outputFormat = new SlsOutputFormat(properties, endPoint, projectName, logstore, serializationSchema);
			}
			outputFormat.setFlushInterval(flushInterval).setFailOnError(failOnError).setMaxRetryTime(maxRetryTimes);
			return outputFormat;
		}

	}

	@Override
	public void configure(Configuration config) {

	}

	@Override
	public long getRetryTimeout() {
		return retryTimeoutMS;
	}

	/**
	 * Callback for sls writer.
	 */
	@VisibleForTesting
	public final class SendFutureCallback implements FutureCallback<Result> {

		@Override
		public void onSuccess(Result result) {
			LOG.debug("loghub-callback: send success, result: "
					+ result.toString());
			if (!result.isSuccessful()) {
				LOG.error("loghub-callback: send failed, result:" + result.toString());
				callBackException = new RuntimeException(result.getErrorMessage());
			}
			numCommitted.incrementAndGet();
		}

		@Override
		public void onFailure(Throwable throwable) {
			if (callBackException == null) {
				LOG.error("loghub-callback: send failed, exception:", throwable);
				if (throwable instanceof ResultFailedException) {
					ResultFailedException exception = ((ResultFailedException) throwable);
					callBackException = new RuntimeException("An exception was thrown, result: " + exception.getResult().toString(), throwable);
				} else {
					callBackException = new RuntimeException("An exception was thrown", throwable);
				}
			}
			numCommitted.incrementAndGet();
		}
	}

	@Override
	public String toString() {
		return String.format("Sls Sink to %s.%s", projectName, logstore);
	}
}
