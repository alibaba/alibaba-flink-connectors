package com.alibaba.flink.connectors.datahub.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.common.util.DateUtil;
import com.alibaba.flink.connectors.datahub.datastream.sink.DatahubRecordResolver;
import com.alibaba.flink.connectors.datahub.datastream.util.DatahubClientProvider;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

import javax.annotation.Nullable;

import java.sql.Date;
import java.sql.Timestamp;

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

	@Nullable
	private final String timeZone;

	public DatahubRowRecordResolver(
			RowTypeInfo flinkRowTypeInfo,
			String project,
			String topic,
			String accessId,
			String accessKey,
			String endpoint,
			@Nullable String timeZone) {
		this.flinkRowTypeInfo = flinkRowTypeInfo;
		this.project = project;
		this.topic = topic;
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
		this.timeZone = timeZone;
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

			TypeInformation info = flinkRowTypeInfo.getTypeAt(i);

			Object columnData = row.getField(i);
			switch (column.getType()) {
				case BIGINT:
				case DECIMAL:
				case BOOLEAN:
					recordData.setField(i, row.getField(i));
					break;
				case DOUBLE:
					if (info.getTypeClass() == Float.class) {
						recordData.setField(i, ((Float) columnData).doubleValue());
					} else {
						recordData.setField(i, columnData);
					}
					break;
				case TIMESTAMP:
					if (columnData == null) {
						recordData.setField(i, null);
					} else {
						if (columnData instanceof Timestamp) {
							recordData.setField(i, ((Timestamp) columnData).getTime());
						} else {
							recordData.setField(i, Long.valueOf(String.valueOf(columnData)));
						}
					}
					break;
				case STRING:
					if (columnData == null) {
						recordData.setField(i, row.getField(i));
					} else {
						if (columnData instanceof Timestamp) {
							recordData.setField(i, DateUtil.timeStamp2String((Timestamp) columnData, timeZone, null));
						} else if (columnData instanceof Date){
							recordData.setField(i, DateUtil.date2String((Date) columnData, timeZone));
						} else {
							recordData.setField(i, String.valueOf(columnData));
						}
					}
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
