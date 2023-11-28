package org.apache.flink.runtime.megaphone;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AckMessage implements Serializable {
	public AckMessage(){

	}

	@Override
	public String toString() {
		return "AckMessage";
	}
}
