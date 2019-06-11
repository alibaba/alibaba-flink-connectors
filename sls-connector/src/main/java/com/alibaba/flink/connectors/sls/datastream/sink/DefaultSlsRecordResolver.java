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

import com.aliyun.openservices.log.common.LogItem;

/**
 * Default serialization schema which wrappers a SlsRecord.
 */
public class DefaultSlsRecordResolver implements SlsRecordResolver<SlsRecord> {
	@Override
	public String getTopic(SlsRecord record) {
		return record.getTopic();
	}

	@Override
	public String getSource(SlsRecord record) {
		return record.getSource();
	}

	@Override
	public String getPartitionKey(SlsRecord record) {
		return record.getPartitionKey();
	}

	@Override
	public LogItem getLogItem(SlsRecord record) {
		return record.getContent();
	}
}
