package backtype.storm.utils;

import java.util.Set;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.TopologyValidator.ValidationResult;


/**
 *  
 *  @author Michael Brailsford (mb013619)
 */
public interface ITopologyValidator {
    boolean isValidationTopology(final StormTopology topology);
    Set<GlobalStreamId> getAllComponentInputs(final StormTopology topology);
    Set<GlobalStreamId> getAllComponentOutputs(final StormTopology topology);
    ValidationResult validateTopology(StormTopology validationTopology);
}
