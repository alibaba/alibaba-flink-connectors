/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.sls.datastream.sink;

import org.apache.flink.configuration.Configuration;

import com.alibaba.flink.connectors.common.sts.AbstractClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sls.com.aliyun.openservices.aliyun.log.producer.LogProducer;
import sls.com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import sls.com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import sls.com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;

/**
 * LogProducer factory.
 */
public class LogProducerProvider extends AbstractClientProvider<LogProducer> {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClientProvider.class);
	private transient ProducerConfig producerConfig;
	private String endPoint;
	private String projectName;
	private int maxRetryTimes;
	private int flushInterval;

	public LogProducerProvider(
			String projectName, String endPoint, String accessId, String accessKey, int maxRetryTimes, int flushInterval) {
		super(accessId, accessKey);
		this.projectName = projectName;
		this.endPoint = endPoint;
		this.maxRetryTimes = maxRetryTimes;
		this.flushInterval = flushInterval;
	}

	public LogProducerProvider(String projectName, String endPoint, Configuration properties, int maxRetryTimes, int flushInterval) {
		super(properties);
		this.projectName = projectName;
		this.endPoint = endPoint;
		this.maxRetryTimes = maxRetryTimes;
		this.flushInterval = flushInterval;
	}

	@Override
	protected void closeClient() {
		if (client != null) {
			// close the producer.
			while (true) {
				try {
					client.close();
					break;
				} catch (InterruptedException e) {
					// ignore interrupt signal to avoid io thread leaking.
				} catch (ProducerException e) {
					LOGGER.warn("Exception caught when closing client", e);
					break;
				}
			}
			client = null;
		}
	}

	@Override
	protected LogProducer produceNormalClient(String accessId, String accessKey) {
		producerConfig = new ProducerConfig();
		producerConfig.setLingerMs(flushInterval);
		producerConfig.setRetries(maxRetryTimes);
		LogProducer producer = new LogProducer(producerConfig);
		ProjectConfig projectConfig = new ProjectConfig(this.projectName, this.endPoint, accessId, accessKey);
		producer.putProjectConfig(projectConfig);
		return producer;
	}

	@Override
	protected LogProducer produceStsClient(String accessId, String accessKey, String securityToken) {
		producerConfig = new ProducerConfig();
		producerConfig.setLingerMs(flushInterval);
		producerConfig.setRetries(maxRetryTimes);
		LogProducer producer = new LogProducer(producerConfig);
		ProjectConfig projectConfig = new ProjectConfig(this.projectName, this.endPoint, accessId, accessKey, securityToken);
		producer.putProjectConfig(projectConfig);
		return producer;
	}
}
