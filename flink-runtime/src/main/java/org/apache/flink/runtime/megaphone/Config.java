package org.apache.flink.runtime.megaphone;

import java.util.Arrays;

public class Config {
	private boolean enable;
	private int delay;
	private int gap;
	private int[] keyGroups;
	private int[] sourceIndex;
	private int[] targetIndex;
	public Config(){
	}

	public Config(boolean enable, int delay, int gap, int[] keyGroups, int[] sourceIndex, int[] targetIndex) {
		this.enable = enable;
		this.delay = delay;
		this.gap = gap;
		this.keyGroups = keyGroups;
		this.sourceIndex = sourceIndex;
		this.targetIndex = targetIndex;
	}

	public boolean isEnable() {
		return enable;
	}

	public void setEnable(boolean enable) {
		this.enable = enable;
	}

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public int getGap() {
		return gap;
	}

	public void setGap(int gap) {
		this.gap = gap;
	}

	public int[] getKeyGroups() {
		return keyGroups;
	}

	public void setKeyGroups(int[] keyGroups) {
		this.keyGroups = keyGroups;
	}

	public int[] getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(int[] sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

	public int[] getTargetIndex() {
		return targetIndex;
	}

	public void setTargetIndex(int[] targetIndex) {
		this.targetIndex = targetIndex;
	}

	@Override
	public String toString() {
		return "Config{" +
			"enable=" + enable +
			", delay=" + delay +
			", gap=" + gap +
			", keyGroups=" + Arrays.toString(keyGroups) +
			", sourceIndex=" + Arrays.toString(sourceIndex) +
			", targetIndex=" + Arrays.toString(targetIndex) +
			'}';
	}
}
