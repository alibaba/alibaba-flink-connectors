package com.alibaba.flink.connectors.datahub.table;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.datahub.datastream.sink.DatahubRecordResolver;
import com.alibaba.flink.connectors.datahub.datastream.util.DatahubClientProvider;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Resolver to convert Flink Row into Datahub RecordEntry.
 */
public class DatahubRowRecordResolver implements DatahubRecordResolver<Row> {
	private final RowTypeInfo flinkRowTypeInfo;
	private final String project;
	private final String topic;
	private final String accessId;
	private final String accessKey;
	private final String endpoint;

	private transient RecordSchema recordSchema;

	public DatahubRowRecordResolver(
			RowTypeInfo flinkRowTypeInfo,
			String project,
			String topic,
			String accessId,
			String accessKey,
			String endpoint) {
		this.flinkRowTypeInfo = flinkRowTypeInfo;
		this.project = project;
		this.topic = topic;
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
	}

	@Override
	public void open() {
		DatahubClient client = new DatahubClientProvider(endpoint, accessId, accessKey).getClient();
		recordSchema = client.getTopic(project, topic).getRecordSchema();

		checkArgument(recordSchema.getFields().size() == flinkRowTypeInfo.getArity());
	}

	@Override
	public RecordEntry getRecordEntry(Row row) {
		RecordEntry record = new RecordEntry();
		TupleRecordData recordData = new TupleRecordData(recordSchema);

		for (int i = 0; i < recordSchema.getFields().size(); i++) {
			Field column = recordSchema.getField(i);
			Object columnData = row.getField(i);
			switch (column.getType()) {
				case BIGINT:
				case DECIMAL:
				case BOOLEAN:
				case DOUBLE:
				case TIMESTAMP:
				case STRING:
					recordData.setField(i, columnData);
					break;
				default:
					throw new RuntimeException(
							String.format("DatahubRowRecordResolver doesn't support type '%s' yet", columnData.getClass().getName()));
			}
		}
		record.setRecordData(recordData);
		return record;
	}
}
