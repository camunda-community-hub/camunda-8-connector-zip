package io.camunda.connector.zip.toolbox;

import com.google.gson.Gson;
import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.filestorage.FileRepoFactory;
import io.camunda.filestorage.FileVariable;
import io.camunda.filestorage.FileVariableReference;
import io.camunda.filestorage.storage.StorageDefinition;
import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;

import java.io.InputStream;

public class ZipToolbox {


    /**
     *
     * @param fileName fileName to write
     * @param mimeType MimeType
     * @param entryStream Entry stream
     * @param storageOutputDefinition Storage definition
     * @param fileRepoFactory fileRepoFactory
     * @param context context
     * @return a file variable reference
     */
    public static FileVariableReference write(String fileName,
                                        String mimeType,
                                        InputStream entryStream,
                                        StorageDefinition storageOutputDefinition,
                                        FileRepoFactory fileRepoFactory,
                                        OutboundConnectorContext context) {

        FileVariable fileVariableOutput = new FileVariable();
        fileVariableOutput.setStorageDefinition(storageOutputDefinition);
        fileVariableOutput.setName(fileName);


        fileVariableOutput.setMimeType(mimeType);

        fileVariableOutput.setValueStream(entryStream);

        // ------- Write it now
        try {
            FileVariableReference fileVariableOutputReference = fileRepoFactory.saveFileVariable(fileVariableOutput, context);
            return fileVariableOutputReference;
        } catch (Exception e) {
            throw new ConnectorException(ZipError.WRITE_FILE,
                    "Error during write file to the output storage " + e.getMessage());
        }
    }

}
