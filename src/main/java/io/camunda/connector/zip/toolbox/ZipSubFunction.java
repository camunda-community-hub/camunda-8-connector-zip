package io.camunda.connector.zip.toolbox;

import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.connector.cherrytemplate.RunnerParameter;
import io.camunda.connector.zip.ZipInput;
import io.camunda.connector.zip.ZipOutput;


import java.util.List;
import java.util.Map;

public interface ZipSubFunction {
    ZipOutput executeSubFunction(ZipInput zipInput,
                                 OutboundConnectorContext context, StringBuilder logExecution) throws ConnectorException;


    List<RunnerParameter> getInputsParameter();

    List<RunnerParameter> getOutputsParameter();

    Map<String, String> getBpmnErrors();

    String getSubFunctionName();

    String getSubFunctionDescription();

    String getSubFunctionType();

}
