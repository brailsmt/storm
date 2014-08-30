package backtype.storm.utils;

import java.util.Set;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;


/**
 * Provide a utility class to validate topologies while they are being built.
 *  
 *  @author Michael Brailsford (michael.brailsford@cerner.com)
 */
public class TopologyValidator implements ITopologyValidator {

    /**
     * Use a Joiner.MapJoiner to display the input and output streams of a component.
     */
    private static final Joiner joiner = Joiner.on(": ");

    private TopologyValidator() {}

    /**
     * Contains the results of validating the topology that is being built.  Contains the set of inputs that do not have
     * a corresponding upstream componentId and streamId and the set of unconsumed outputs from each component in the
     * topology.  ValidationResult instances are immutable.
     */
    public static class ValidationResult {
        /**
         * The set of declared inputs that do not have a corresponding upstream componentId and streamId.  These will
         * cause an InvalidTopologyExeption to be thrown when the topology is submitted to Nimbus.
         */
        private final ImmutableSet<GlobalStreamId> invalidInputs;

        /**
         * The set of declared outputs that do not have a corresponding downstream consumer.  These will not cause an
         * error when submitting the topology, but this information is useful for conumsers building a topology.
         */
        private final ImmutableSet<GlobalStreamId> unconsumedOutputs;

        /**
         * Create a new ValidationResult.
         * 
         * @param invalidInputs The set of invalid inputs.
         * @param unconsumedOutputs The set of unconsumed outputs
         */
        public ValidationResult(Set<GlobalStreamId> invalidInputs, Set<GlobalStreamId> unconsumedOutputs) {
            this.invalidInputs     = ImmutableSet.copyOf(invalidInputs);
            this.unconsumedOutputs = ImmutableSet.copyOf(unconsumedOutputs);
        }

        /**
         * Return the set of inputs that do not have an upstream output.
         * 
         * @return ImmutableSet of GlobalStreamId's for each invalid input.
         */
        public ImmutableSet<GlobalStreamId> getInvalidInputs() {
            return invalidInputs;
        }

        /**
         * Return the set of outputs that are defined, but not consumed by downstream components.
         * 
         * @return ImmutableSet of GlobalStreamId for each unconsumed output declared.
         */
        public ImmutableSet<GlobalStreamId> getUnconsumedOutputs() {
            return unconsumedOutputs;
        }
    }

    /**
     * Determine if the topology provided is a topology built only for validation.  Such topologies are created with
     * TopologyBuilder.createValidationTopology().  A Validation Topology has no bolt_objects, spout_objects or
     * state_spout_objects set.
     *
     * @param topology The topology to check
     * @return true if the topology contains a bolt or spout with a null ComponentObject
     */
    public boolean isValidationTopology(final StormTopology topology) {
        for(final Bolt bolt: topology.get_bolts().values()) {
            if (bolt.get_bolt_object() == null) {
                return true;
            }
        }
        for(final SpoutSpec spout: topology.get_spouts().values()) {
            if(spout.get_spout_object() == null) {
                return true;
            }
        }
        for(final StateSpoutSpec stateSpoutSpec: topology.get_state_spouts().values()) {
            if(stateSpoutSpec.get_state_spout_object() == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Provide a list of every GlobalStreamId for every input consumed by some component in the topology.
     *
     * @param topology The topology for which all inputs should be returned.
     *
     * @return A set of all GlobalStreamIds to which bolts have subscribed
     */
    public Set<GlobalStreamId> getAllComponentInputs(final StormTopology topology) {
        final Set<GlobalStreamId> allInputs = Sets.newHashSet();
        ComponentCommon componentCommon = null;
        for(final String componentId: Utils.getAllComponentIds(topology)) {
            componentCommon = Utils.getComponentCommon(topology, componentId);
            allInputs.addAll(componentCommon.get_inputs().keySet());
        }

        return allInputs;
    }

    /**
     * Provide a list of every GlobalStreamId for every output declared by every component in the topology.
     *
     * @param topology The topology for which all outputs should be returned.
     *
     * @return A set of all GlobalStreamIds that have been declared by components in this topology
     */
    public Set<GlobalStreamId> getAllComponentOutputs(final StormTopology topology) {
        final Set<GlobalStreamId> allOutputs = Sets.newHashSet();
        ComponentCommon componentCommon;
        for(final String componentId: Utils.getAllComponentIds(topology)) {
            componentCommon = Utils.getComponentCommon(topology, componentId);
            for(String streamId: componentCommon.get_streams().keySet()) {
                allOutputs.add(new GlobalStreamId(componentId, streamId));
            }
        }

        return allOutputs;
    }

    /**
     * Identify inputs defined in the topology that do not have a corresponding output listed, also determine
     * unconsumedOutputs.  Inputs without a matching output will cause an InvalidTopologyException to be thrown when the
     * topology is submitted to Nimbus.  Outputs that are unconsumed will not cause an error, but the information may be
     * useful to topology builders.
     *
     * @param validationTopology The topology to be validated.  This can be a topology built with either
     *                           TopologyBuilder.createTopology() or TopologyBuilder.createValidationTopology().
     *
     * @return A ValidationResult the contains the sets of invalid inputs and unconsumed outputs.
     */
    public ValidationResult validateTopology(StormTopology validationTopology) {
        final Set<GlobalStreamId> inputs = getAllComponentInputs(validationTopology);
        final Set<GlobalStreamId> outputs = getAllComponentOutputs(validationTopology);

        return new ValidationResult(Sets.difference(inputs, outputs), Sets.difference(outputs, inputs));
    }


    /**
     * Display the input streams for a given ComponentCommon object and all the output streams it has declared.  This
     * is useful for debugging topologies.
     */
    public String componentStreamsToString(final StormTopology topology, final String componentId) {
        ComponentCommon componentCommon = Utils.getComponentCommon(topology, componentId);
        StringBuilder rv = new StringBuilder("input (component, stream):  ");
        for(GlobalStreamId input: componentCommon.get_inputs().keySet()) {
            rv.append("(").append(input.get_componentId()).append(", ").append(input.get_streamId()).append(") ");
        }
        rv.append("\noutput (component, stream):  ");
        for(String streamId: componentCommon.get_streams().keySet()) {
            rv.append("(").append(componentId).append(", ").append(streamId).append(") ");
        }

        return rv.toString();
    }
}
