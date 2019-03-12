package com.hazelcast.jet.beam;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.beam.JetRunner.JetPipelineResult;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import org.apache.beam.runners.core.construction.UnconsumedReads;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class JetRunner extends PipelineRunner<JetPipelineResult> {

    /**
     * Constructs a runner from the provided {@link PipelineOptions}.
     *
     * @return The newly created runner.
     */
    public static JetRunner fromOptions(PipelineOptions options) {
        return new JetRunner(options);
    }

    private static final Logger LOG = LoggerFactory.getLogger(JetRunner.class);

    private final JetRunnerOptions options;

    private JetRunner(PipelineOptions options) {
        this.options = validate(options.as(JetRunnerOptions.class));
    }

    public JetPipelineResult run(Pipeline pipeline) {
        normalize(pipeline);
        DAG dag = translate(pipeline);
        return run(dag);
    }

    private void normalize(Pipeline pipeline) {
        pipeline.replaceAll(getDefaultOverrides());
        UnconsumedReads.ensureAllReadsConsumed(pipeline);
    }

    private DAG translate(Pipeline pipeline) {
        /*PrintGraphVisitor printVisitor = new PrintGraphVisitor();
        pipeline.traverseTopologically(printVisitor);
        System.out.println("Beam pipeline:" + printVisitor.print()); //todo: remove*/

        /*PrintFullGraphVisitor printFullVisitor = new PrintFullGraphVisitor();
        pipeline.traverseTopologically(printFullVisitor);
        System.out.println("Beam pipeline:" + printFullVisitor.print()); //todo: remove*/

        JetGraphVisitor graphVisitor = new JetGraphVisitor(options);
        pipeline.traverseTopologically(graphVisitor);
        return graphVisitor.getDAG();
    }

    private JetPipelineResult run(DAG dag) {
        JetPipelineResult result = new JetPipelineResult();
        JetInstance jet = getJetInstance(options);
        Job job = jet.newJob(dag);
        result.setJob(job);
        job.join();
        return result;
    }

    private static List<PTransformOverride> getDefaultOverrides() {
        //todo: Combine.globally(SumLongs) is built up from 7 nodes... see Combine.Globally.expand()
        //todo: more replacements?
        return Collections.emptyList();
    }

    private static JetRunnerOptions validate(JetRunnerOptions options) {
        if (options.getJetGroupName() == null) throw new IllegalArgumentException("Jet group NAME not set in options!");
        if (options.getJetGroupPassword() == null) throw new IllegalArgumentException("Jet group PASSWORD not set in options!");
        return options;
    }

    private static JetInstance getJetInstance(JetRunnerOptions options) {
        String jetGroupName = options.getJetGroupName();
        String jetGroupPassword = options.getJetGroupPassword();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(jetGroupName);
        clientConfig.getGroupConfig().setPassword(jetGroupPassword);
        return Jet.newJetClient(clientConfig);
    }

    public static class JetPipelineResult implements PipelineResult {

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

}
