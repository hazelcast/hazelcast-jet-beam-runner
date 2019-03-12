package com.hazelcast.jet.beam;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JetPipelineResult implements PipelineResult {

    private static final Logger LOG = LoggerFactory.getLogger(JetRunner.class);

    private Job job;

    void setJob(Job job) {
        this.job = job;
    }

    public State getState() {
        if (job == null) return State.UNKNOWN;
        return getState(job);
    }

    public State cancel() throws IOException {
        if (job == null) return State.UNKNOWN; //todo: what to do?

        job.cancel();
        return getState(job);
    }

    public State waitUntilFinish(Duration duration) {
        if (job == null) return State.UNKNOWN; //todo: what to do?

        return waitUntilFinish(); //todo: how to time out?
    }

    public State waitUntilFinish() {
        try {
            job.join();
        } catch (Exception e) {
            e.printStackTrace(); //todo: what to do?
            return State.FAILED;
        }

        return getState(job);
    }

    public MetricResults metrics() {
        return null; //todo
    }

    private static State getState(Job job) {
        JobStatus status = job.getStatus();
        switch (status) {
            case COMPLETED:
                return State.DONE;
            case COMPLETING:
            case RUNNING:
            case STARTING:
                return State.RUNNING;
            case FAILED:
                return State.FAILED;
            case NOT_RUNNING:
            case SUSPENDED:
                return State.STOPPED;
            default:
                LOG.warn("Unhandled " + JobStatus.class.getSimpleName() + ": " + status.name() + "!");
                return State.UNKNOWN;
        }
    }
}
