package org.apache.flink.runtime.megaphone;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;

public class RpcMegaphoneControllerResponder {
	private static JobMasterGateway jobMasterGateway;
	private String taskname;

	public RpcMegaphoneControllerResponder(JobMasterGateway jobMasterGateway){
		this.jobMasterGateway = jobMasterGateway;
	}

	public void acknowledgeToController(JobID jobID, ExecutionAttemptID executionAttemptID, String taskname){
		jobMasterGateway.acknowledgeToController(jobID, executionAttemptID, taskname);
	}

	public void acknowledgeToController(JobID jobID, ExecutionAttemptID executionAttemptID, String taskname, AckMessage ack){
		jobMasterGateway.acknowledgeToController(jobID, executionAttemptID, taskname, ack);
	}


	public void setTaskname(String taskname) {
		this.taskname = taskname;
	}
}
