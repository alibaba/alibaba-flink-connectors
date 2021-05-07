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

import com.alibaba.flink.connectors.common.exception.ErrorUtils;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


class SlsClientProxy {
    private static final Logger LOG = LoggerFactory.getLogger(SlsClientProxy.class);

    private static final String ERROR_SHARD_NOT_EXIST = "ShardNotExist";
    private static final String ERROR_CONSUMER_GROUP_EXIST = "ConsumerGroupAlreadyExist";
    private static final int MAX_RETRIES_FOR_NON_SERVER_ERROR = 3;
    private static final long MAX_RETRY_TIMEOUT_MILLIS = 10 * 60 * 1000;

    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String project;
    private String logstore;
    private String consumerGroup;
    private Configuration properties;
    private boolean directMode;
    private transient SlsClientProvider clientProvider = null;

    public SlsClientProxy(String endpoint,
                          String accessKeyId,
                          String accessKeySecret,
                          String project,
                          String logstore,
                          String consumerGroup,
                          Configuration properties) {
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.project = project;
        this.logstore = logstore;
        this.consumerGroup = consumerGroup;
        this.properties = properties;
        this.directMode = false;
    }

    private SlsClientProvider getSlsClientProvider() {
        if (null == clientProvider) {
            if (null != accessKeyId && null != accessKeySecret && !accessKeyId.isEmpty() &&
                    !accessKeySecret.isEmpty()) {
                clientProvider = new SlsClientProvider(
                        endpoint,
                        accessKeyId,
                        accessKeySecret,
                        consumerGroup,
                        directMode);
            } else {
                clientProvider = new SlsClientProvider(
                        endpoint,
                        properties,
                        consumerGroup,
                        directMode);
            }
        }
        return clientProvider;
    }

    public static class RequestContext {
        private final long beginTime;
        private int attempt;

        public RequestContext() {
            this.beginTime = System.currentTimeMillis();
            this.attempt = 0;
        }

        public boolean waitForNextRetry(LogException ex) {
            if (!shouldRetry(ex)) {
                return false;
            }
            LOG.warn("Retrying for recoverable exception code = {}, message = {}, attempt={}",
                    ex.GetErrorCode(), ex.GetErrorMessage(), attempt);
            ++attempt;
            try {
                Thread.sleep(attempt * 1000);
            } catch (InterruptedException iex) {
                LOG.warn("Sleep interrupted", iex);
            }
            return true;
        }

        private boolean shouldRetry(LogException ex) {
            int status = ex.GetHttpCode();
            boolean isNetworkError = status == -1;
            boolean isServerError = status >= 500;
            boolean canRetry = isNetworkError
                    || isServerError
                    || (status != 400 && attempt < MAX_RETRIES_FOR_NON_SERVER_ERROR);
            return canRetry && System.currentTimeMillis() - beginTime <= MAX_RETRY_TIMEOUT_MILLIS;
        }
    }

    List<Shard> listShards() throws LogException {
        Client client = getSlsClientProvider().getClient();
        RequestContext ctx = new RequestContext();
        while (true) {
            try {
                return client.ListShard(project, logstore).GetShards();
            } catch (LogException ex) {
                if (ctx.waitForNextRetry(ex)) {
                    continue;
                }
                LOG.error("Error while listing shards, code = {}", ex.GetErrorCode(), ex);
                // We cannot start from empty shard list.
                throw ex;
            }
        }
    }

    String getCursor(Consts.CursorMode cursorMode, int shard) throws LogException {
        Client client = getSlsClientProvider().getClient();
        RequestContext ctx = new RequestContext();
        while (true) {
            try {
                return client.GetCursor(project, logstore, shard, cursorMode).GetCursor();
            } catch (LogException ex) {
                if (ctx.waitForNextRetry(ex)) {
                    continue;
                }
                LOG.error("Error while fetching cursor, code = {}", ex.GetErrorCode(), ex);
                // We cannot start from null cursor.
                throw ex;
            }
        }
    }

    String getCursor(int ts, int shard) throws LogException {
        Client client = getSlsClientProvider().getClient();
        RequestContext ctx = new RequestContext();
        while (true) {
            try {
                return client.GetCursor(project, logstore, shard, ts).GetCursor();
            } catch (LogException ex) {
                if (ctx.waitForNextRetry(ex)) {
                    continue;
                }
                LOG.error("Error while fetching cursor, code = {}", ex.GetErrorCode(), ex);
                // We cannot start from null cursor.
                throw ex;
            }
        }
    }

    int getCursorTime(int shard, String cursor) {
        Client client = getSlsClientProvider().getClient();
        RequestContext ctx = new RequestContext();
        while (true) {
            try {
                return client.GetCursorTime(project, logstore, shard, cursor).GetCursorTime();
            } catch (LogException ex) {
                if (ctx.waitForNextRetry(ex)) {
                    continue;
                }
                LOG.error("Error while fetching cursor time", ex);
                return -1;
            }
        }
    }

    void updateCheckpoint(int shard, String cursor) {
        Client client = getSlsClientProvider().getClient();
        RequestContext ctx = new RequestContext();
        while (true) {
            try {
                client.UpdateCheckPoint(project, logstore, consumerGroup, shard, cursor);
                break;
            } catch (LogException ex) {
                if (ctx.waitForNextRetry(ex)) {
                    continue;
                } else if (ERROR_SHARD_NOT_EXIST.equalsIgnoreCase(ex.GetErrorCode())) {
                    LOG.warn("Shard not exist, skip checkpointting, message = {}", ex.GetErrorMessage());
                    return;
                }
                LOG.error("Error while updating checkpoint", ex);
                break;
            }
        }
    }

    BatchGetLogResponse pullData(int shard, int batchSize, String fromCursor, String endCursor) throws LogException {
        Client client = getSlsClientProvider().getClient();
        RequestContext ctx = new RequestContext();
        while (true) {
            try {
                return client.BatchGetLog(project, logstore, shard, batchSize, fromCursor, endCursor);
            } catch (LogException ex) {
                if (ctx.waitForNextRetry(ex)) {
                    continue;
                }
                LOG.error("Error while pulling data from SLS", ex);
                throw ex;
            }
        }
    }

    void refresh() {
        getSlsClientProvider().getClient(true, true);
    }

    void ensureConsumerGroupCreated() {
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            return;
        }
        Client client = getSlsClientProvider().getClient();
        RequestContext ctx = new RequestContext();
        while (true) {
            try {
                ConsumerGroup group = new ConsumerGroup(consumerGroup, 60, false);
                client.CreateConsumerGroup(project, logstore, group);
            } catch (LogException ex) {
                if (ERROR_CONSUMER_GROUP_EXIST.equalsIgnoreCase(ex.GetErrorCode())) {
                    LOG.info("Consumer group {} exist, no need to create it", consumerGroup);
                    return;
                }
                // Retry for 500 error only here as failover is OK as we're not starting
                if (ctx.waitForNextRetry(ex)) {
                    continue;
                }
                ErrorUtils.throwException(
                        "error occur when create consumer group, errorCode: " + ex.GetErrorCode() +
                                ", errorMessage: " + ex.GetErrorMessage());
            }
        }
    }

    public void setDirectMode(boolean directMode) {
        this.directMode = directMode;
    }
}
