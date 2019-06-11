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

import java.io.Serializable;

/**
 * SlsRecord wrappers content and meta data of the record to write to Sls.
 */
public class SlsRecord implements Serializable {

	private final String topic;
	private final String source;
	private final String partitionKey;
	private final LogItem content;

	public SlsRecord(LogItem content, String topic, String source, String partitionKey) {
		this.content = content;
		this.topic = topic;
		this.source = source;
		this.partitionKey = partitionKey;
	}

	public SlsRecord(LogItem content) {
		this(content, "", "", null);
	}

	public String getTopic() {
		return topic;
	}

	public String getSource() {
		return source;
	}

	public String getPartitionKey() {
		return partitionKey;
	}

	public LogItem getContent() {
		return content;
	}

}
