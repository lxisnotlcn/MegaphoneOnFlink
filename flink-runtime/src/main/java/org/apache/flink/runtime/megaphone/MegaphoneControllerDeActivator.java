package org.apache.flink.runtime.megaphone;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MegaphoneControllerDeActivator implements JobStatusListener {
	private final MegaphoneController megaphoneController;

	public MegaphoneControllerDeActivator(MegaphoneController megaphoneController){
		this.megaphoneController = checkNotNull(megaphoneController);
	}


	@Override
	public void jobStatusChanges(
		JobID jobId,
		JobStatus newJobStatus,
		long timestamp,
		Throwable error) {
		if(newJobStatus == JobStatus.RUNNING){
			megaphoneController.startScheduler();
		}else{
			megaphoneController.stopScheduler();
		}
	}
}
