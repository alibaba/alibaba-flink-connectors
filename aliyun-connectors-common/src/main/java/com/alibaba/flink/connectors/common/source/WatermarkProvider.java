package com.alibaba.flink.connectors.common.source;

/**
 * WatermarkProvider.
 */
public interface WatermarkProvider {
	/**
	 * Gets current partition watermark.
	 * @return
	 */
	long getWatermark();
}
