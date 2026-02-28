package io.camunda.connector.zip;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.cherrytemplate.CherryInput;
import io.camunda.connector.cherrytemplate.RunnerParameter;
import io.camunda.connector.zip.toolbox.ParameterToolbox;
import io.camunda.connector.zip.toolbox.ZipError;
import io.camunda.filestorage.FileVariable;
import io.camunda.filestorage.storage.StorageDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * the JsonIgnoreProperties is mandatory: the template may contain additional widget to help the designer, especially on the OPTIONAL parameters
 * This avoids the MAPPING Exception
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZipInput implements CherryInput {
    public static final String GROUP_INPUT_FILE = "Input file";
    public static final String GROUP_PARAMETERS = "Parameters";

    public static final String GROUP_STORAGE_DEFINITION = "Destination Storage";
    /**
     * Attention, each Input here must be added in the function, list of InputVariables
     */
    public static final String ZIP_FUNCTION = "zipFunction";
    public static final String SOURCE_FILE = "sourceFile";
    public static final String LIST_SOURCE_FILE = "listSourceFile";
    public static final String STOP_AT_FIRST_ERROR = "stopAtFirstError";

    public static final String DESTINATION_JSON_STORAGE_DEFINITION = "destinationJsonStorageDefinition";
    public static final String FILTER_PATH = "filterPath";
    public static final String FILTER_FILE = "filterFile";
    public static final String KEEP_FOLDER_STRUCTURE = "filterFile";
    public static final String SORT_ZIP_ENTRIES = "sortZipEntries";
    public static final String SORT_ZIP_ENTRIES_V_NONE = "NONE";
    public static final String SORT_ZIP_ENTRIES_V_NUMBER = "NUMBER";
    public static final String SORT_ZIP_ENTRIES_V_ASCII = "ASCII";

    public static final String ENCODING = "encoding";
    public static final String ZIP_FILENAME = "zipFileName";
    public static final String COMPRESS_FORMAT = "compressFormat";


    public static final RunnerParameter zipParameterDestinationJsonStorageDefinition = new RunnerParameter(
            ZipInput.DESTINATION_JSON_STORAGE_DEFINITION, // name
            "JSon Storage Destination", // label
            Map.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Storage Definition in Json.").setGroup(GROUP_STORAGE_DEFINITION);

    public String zipFunction;
    public Object sourceFile;
    public List<Object> listSourceFile;

    public Object destinationJsonStorageDefinition;

    public String filterPath;
    public String filterFile;
    public Boolean keepFolderStructure;
    public String encoding;
    public String zipFileName;
    public Boolean stopAtFirstError = Boolean.TRUE;

    public String sortZipEntries;
    public String compressFormat;

    private final Logger logger = LoggerFactory.getLogger(ZipInput.class.getName());

    public Object getSourceFile() {
        return sourceFile;
    }

    public List<Object> getListSourceFile() {
        return listSourceFile;
    }


    public Charset getEncoding() {
        if (encoding == null || encoding.trim().isEmpty())
            return StandardCharsets.UTF_8;
        return Charset.forName(encoding);

    }

    public String getZipFileName() {
        return zipFileName;
    }

    public Object getDestinationJsonStorageDefinition() {
        return destinationJsonStorageDefinition;
    }

    public String getZipFunction() {
        return zipFunction;
    }

    public String getFilterFile() {
        return filterFile;
    }

    public Boolean getKeepFolderStructure() {
        return keepFolderStructure;
    }

    public enum CompressFormat {
        ZIP, RAR
    }

    public boolean getStopAtFirstError() {
        return stopAtFirstError;
    }

    public enum SORTENTRY {NONE, NUMBER, NUMBERASCII, ASCII}

    public SORTENTRY getSortZipEntry() {
        try {
            return SORTENTRY.valueOf(sortZipEntries);
        } catch (
                Exception e) {
            return SORTENTRY.NONE;
        }
    }

    public CompressFormat getCompressFormat() {
        if (compressFormat == null)
            return CompressFormat.ZIP;
        try {
            return CompressFormat.valueOf(compressFormat);
        } catch (
                Exception e) {
            return null;
        }
    }

    @JsonIgnore
    @Override
    public List<Map<String, Object>> getInputParameters() {
        return ParameterToolbox.getInputParameters();
    }


    public FileVariable initializeOutputFileVariable(String fileName) throws ConnectorException {
        StorageDefinition storageOutputDefinition = getDestinationStorageDefinitionObject();

        FileVariable fileVariable = new FileVariable();
        fileVariable.setStorageDefinition(storageOutputDefinition);
        fileVariable.setName(fileName);
        return fileVariable;
    }

    /**
     * Return a Storage definition
     *
     * @return the storage definition
     * @throws ConnectorException if the connection
     */
    @JsonIgnore
    public StorageDefinition getDestinationStorageDefinitionObject() throws ConnectorException {
        try {
            StorageDefinition storageDefinitionObj = StorageDefinition.getFromObject(destinationJsonStorageDefinition);
            return storageDefinitionObj;
        } catch (Exception e) {
            logger.error("Can't get the FileStorage - bad Json value :" + destinationJsonStorageDefinition);
            throw new ConnectorException(ZipError.INCORRECT_STORAGEDEFINITION,
                    "FileStorage information: " + destinationJsonStorageDefinition);
        }
    }

}
