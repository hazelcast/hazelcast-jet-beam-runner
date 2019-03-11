package com.hazelcast.jet.beam;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface JetRunnerOptions extends PipelineOptions {

    @Description("Name of Jet group")
    @Validation.Required
    String getJetGroupName();
    void setJetGroupName(String jetGroupName);

    @Description("Password of Jet group")
    @Validation.Required
    String getJetGroupPassword();
    void setJetGroupPassword(String jetGroupPassword); //todo: Jet group password not needed, deprecated

    //todo: I guess more params are require
    //todo: would be nicer to directly pass in the JetInstance, but I can't find a way to get that through the Beam config mechanisms

}
