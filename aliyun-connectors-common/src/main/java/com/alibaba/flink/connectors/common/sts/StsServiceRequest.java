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

package com.alibaba.flink.connectors.common.sts;

import org.apache.flink.configuration.Configuration;

import com.alibaba.flink.connectors.common.conf.BlinkOptions;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.X509TrustAll;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithServiceIdentityRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithServiceIdentityResponse;

import static com.alibaba.flink.connectors.common.sts.StsConstants.DURATION;
import static com.alibaba.flink.connectors.common.sts.StsConstants.PROTOCOL_TYPE;

/**
 * StsServiceRequest.
 */
public class StsServiceRequest {
	private static final String INNER_STS_ENDPOINT = "__inner__blink_sts_endpoints__";

	public static AssumeRoleWithServiceIdentityResponse assumeRoleWithServiceIdentity(
			final String streamAccessId, final String streamAccessKey,
			final String roleArn, final String roleSessionName,
			final String assumeRoleFor,
			Configuration properties) throws Exception {
		//decode
		String decodeKey = DecodeUtil.decrypt(streamAccessKey, StsConstants.STS_SECRET_KEY);

		String regionId = properties.getString(BlinkOptions.STS.STS_REGION_ID);

		// 创建一个 Aliyun Acs Client, 用于发起 OpenAPI 请求
		IClientProfile profile = DefaultProfile.getProfile(
				regionId, streamAccessId, decodeKey);
		DefaultAcsClient client = new DefaultAcsClient(profile);

		// endPoints format:   endPointName#regionId#product#domain,endPointName1#regionId1#product1#domain1
		if (properties.containsKey(INNER_STS_ENDPOINT) && properties.getString(INNER_STS_ENDPOINT, null) != null){
			String endPoints = properties.toMap().get(INNER_STS_ENDPOINT);
			if (!endPoints.isEmpty()) {
				String[] endPointItem = endPoints.split(",");
				for (String item : endPointItem) {
					String[] partItems = item.split("#");
					if (null != partItems && partItems.length == 4) {
						DefaultProfile.addEndpoint(partItems[0], partItems[1], partItems[2], partItems[3]);
					}
				}
			}
		}

		// 创建一个 AssumeRoleRequest 并设置请求参数
		final AssumeRoleWithServiceIdentityRequest request = new AssumeRoleWithServiceIdentityRequest();
		request.setMethod(MethodType.POST);

		request.setProtocol(PROTOCOL_TYPE);
		request.setDurationSeconds(DURATION);
		request.setRoleArn(roleArn);
		request.setRoleSessionName(roleSessionName);
		request.setAssumeRoleFor(assumeRoleFor);
		X509TrustAll.ignoreSSLCertificate();
		// 发起请求，并得到response
		final AssumeRoleWithServiceIdentityResponse response = client.getAcsResponse(request);

		return response;
	}
}
