package org.apache.flink.runtime.megaphone;

public class UpAckMessage extends AckMessage{
	private Long watermark;
	private int index;

	public UpAckMessage(){
		this.watermark = -1L;
		this.index = -1;
	}

	public UpAckMessage(Long watermark, int index){
		this.watermark = watermark;
		this.index = index;
	}

	public Long getWatermark() {
		return watermark;
	}

	public void setWatermark(Long watermark) {
		this.watermark = watermark;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	@Override
	public String toString() {
		return "UpAckMessage{" +
			"watermark=" + watermark +
			", index=" + index +
			'}';
	}
}
