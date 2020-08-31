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

import org.apache.flink.configuration.Configuration;

import com.alibaba.flink.connectors.common.sts.AbstractClientProvider;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;

import java.net.InetAddress;

/**
 * Sls client factory.
 */
public class SlsClientProvider extends AbstractClientProvider<Client> {

	private String endPoint = null;
	private String consumerGroup = null;
	private boolean directMode = false;

	public SlsClientProvider(
			String endPoint,
			String accessKeyId,
			String accessKeySecret,
			String consumerGroup,
			boolean directMode) {
		super(accessKeyId, accessKeySecret);
		this.endPoint = endPoint;
		this.consumerGroup = consumerGroup;
		this.directMode = directMode;
		Consts.HTTP_SEND_TIME_OUT = 10 * 1000;
	}

	public SlsClientProvider(
			String endPoint,
			Configuration properties,
			String consumerGroup,
			boolean directMode) {
		super(properties);
		this.endPoint = endPoint;
		this.consumerGroup = consumerGroup;
		this.directMode = directMode;
		Consts.HTTP_SEND_TIME_OUT = 10 * 1000;
	}

	@Override
	protected void closeClient() {
	}

	@Override
	protected Client produceNormalClient(String accessId, String accessKey) {
		Client client = new Client(endPoint, accessId, accessKey);
		if (directMode){
			client.EnableDirectMode();
		}
		client.setUserAgent("Blink-ak" + "-" + consumerGroup + "-" +
							getHostName());
		return client;
	}

	@Override
	protected Client produceStsClient(String accessId, String accessKey, String securityToken) {
		Client client = new Client(endPoint, accessId, accessKey);
		client.setUserAgent("Blink-sts" + "-" + consumerGroup + "-" +
							getHostName());
		client.SetSecurityToken(securityToken);
		return client;
	}

	private String getHostName() {
		String ip = "";
		try {
			InetAddress addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress();
		} catch (Exception e) {
			//ignore
		}
		return String.format("%s", ip);
	}
}
