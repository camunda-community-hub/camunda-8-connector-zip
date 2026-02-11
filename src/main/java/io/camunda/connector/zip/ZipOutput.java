package io.camunda.connector.zip;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.camunda.connector.cherrytemplate.CherryOutput;
import io.camunda.connector.zip.toolbox.ParameterToolbox;
import io.camunda.filestorage.FileVariableReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ZipOutput implements CherryOutput {
    public static final String LIST_DOCUMENTS = "listDocuments";
    public List<FileVariableReference> listDocumentsId = new ArrayList<>();
    public FileVariableReference zipFile = null;


    @JsonIgnore
    @Override
    public List<Map<String, Object>> getOutputParameters() {
        return ParameterToolbox.getOutputParameters();
    }


}
