/* ******************************************************************** */
/*                                                                      */
/*  CreateFolderWorker                                                   */
/*                                                                      */
/*  Create a folder in CMIS      */
/* ******************************************************************** */
package io.camunda.connector.zip.zipfunction;

import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.connector.cherrytemplate.RunnerParameter;
import io.camunda.connector.zip.ZipInput;
import io.camunda.connector.zip.ZipOutput;
import io.camunda.connector.zip.toolbox.ZipError;
import io.camunda.connector.zip.toolbox.ZipSubFunction;
import io.camunda.connector.zip.toolbox.ZipToolbox;
import io.camunda.filestorage.FileRepoFactory;
import io.camunda.filestorage.FileVariable;
import io.camunda.filestorage.FileVariableReference;
import io.camunda.filestorage.storage.StorageDefinition;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class ZipFilesSubFunction implements ZipSubFunction {
    public static final String TYPE_ZIP_ZIP = "zip";
    private final Logger logger = LoggerFactory.getLogger(ZipFilesSubFunction.class.getName());

    public ZipFilesSubFunction() {
        // No special to add here
    }

    @Override
    public ZipOutput executeSubFunction(ZipInput zipInput,
                                        OutboundConnectorContext context, StringBuilder logExecution) throws ConnectorException {

        if (zipInput.getCompressFormat().equals(ZipInput.CompressFormat.RAR)) {
            throw new ConnectorException(ZipError.RAR_COMPRESSION_NOT_SUPPORTED, "Rar compression is not supported");
        }


        ZipOutput zipOutput = new ZipOutput();
        FileRepoFactory fileRepoFactory = FileRepoFactory.getInstance();
        ByteArrayOutputStream zipBufferOutputStream = new ByteArrayOutputStream();
        ZipArchiveOutputStream zipOut = new ZipArchiveOutputStream(zipBufferOutputStream);
        zipOut.setEncoding(zipInput.getEncoding().name());
        zipOut.setMethod(ZipArchiveOutputStream.DEFLATED);

        // read ListSourceFile
        try {
            int count = 0;
            StorageDefinition defaultStorageDefinition=null;
            for (Object file : zipInput.getListSourceFile()) {
                count++;
                FileVariableReference fileVariableReference;
                FileVariable fileVariable = null;
                logExecution.append("Zip ");
                try {
                    fileVariableReference = FileVariableReference.fromObject(file);
                    fileVariable = fileRepoFactory.loadFileVariable(fileVariableReference, context);

                    if (defaultStorageDefinition==null)
                        defaultStorageDefinition=fileVariable.getStorageDefinition();
                    InputStream inputStreamFile = fileVariable.getValueStream();

                    // get a filename
                    String fileName = fileVariableReference.getFileName();
                    if (fileName == null)
                        fileName = "File " + count;
                    if (fileName.length()>40)
                        fileName = fileName.substring(0, 40) +count;

                    // ZIP the file
                    ZipArchiveEntry zipEntry = new ZipArchiveEntry(fileName);
                    zipOut.putArchiveEntry(zipEntry);
                    inputStreamFile.transferTo(zipOut);

                    logExecution.append(String.format("[%s],", fileName));
                    logger.debug("{}/{} file [{}] ", count, zipInput.getListSourceFile().size(), fileName);
                    zipOut.closeArchiveEntry();
                } catch (Exception e) {
                    logger.error("Upload error on file {}", fileVariable != null ? fileVariable.getName() : "", e);
                    logExecution.append(String.format("Error on [{}] : {}", fileVariable != null ? fileVariable.getName() : "", e.getMessage()));
                    if (zipInput.getStopAtFirstError()) {
                        throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file [" + zipInput.getSourceFile()
                                + " :" + e.getMessage());
                    }

                }
            }

            zipOut.finish();
            InputStream zipBufferInputStream = new ByteArrayInputStream(zipBufferOutputStream.toByteArray());

            StorageDefinition storageDestination = zipInput.getDestinationStorageDefinitionObject();
            if (storageDestination == null) {
                storageDestination = defaultStorageDefinition;
            }
            if (storageDestination == null) {
                throw new ConnectorException(ZipError.INCORRECT_STORAGEDEFINITION, "Specify a storage definition");
            }
            zipOutput.zipFile = ZipToolbox.write(zipInput.getZipFileName(), "application/zip", zipBufferInputStream, zipInput.getDestinationStorageDefinitionObject(), fileRepoFactory, context);
            logExecution.append(String.format("], Write [%s]", zipInput.getZipFileName()));

        } catch (ConnectorException ce) {
            throw ce;
        } catch (Exception e) {
            logger.error("Upload error on file {}", zipInput.getZipFileName(), e);
            throw new ConnectorException(ZipError.WRITE_FILE, "Can't write the file [" + zipInput.getZipFileName()
                    + " :" + e.getMessage());
        } finally {
            try {
                zipOut.finish();
            } catch (IOException e) {
                // Do nothing
            }
        }


        return zipOutput;


    }

    @Override
    public List<RunnerParameter> getInputsParameter() {
        return List.of(
                new RunnerParameter(ZipInput.LIST_SOURCE_FILE, "List Source files", List.class,
                        RunnerParameter.Level.REQUIRED, "Variable who contains the list of files to be stored in the Zip file")
                        .setGroup(ZipInput.GROUP_INPUT_FILE),
                /* Only ZIP is supported for the moment
                new RunnerParameter(ZipInput.COMPRESS_FORMAT, "Compress format", String.class,
                        RunnerParameter.Level.OPTIONAL, "Compression: ATTENTION ONLY ZIP us supported")
                        .addChoice(ZipInput.CompressFormat.ZIP.name(), ZipInput.CompressFormat.ZIP.name())//
                        .addChoice(ZipInput.CompressFormat.RAR.name(), ZipInput.CompressFormat.RAR.name())//
                        .setVisibleInTemplate()
                        .setDefaultValue(ZipInput.CompressFormat.ZIP.name()),
                */
                new RunnerParameter(ZipInput.ENCODING, "Encoding (Chartset", String.class, RunnerParameter.Level.OPTIONAL,
                        "Encoding")
                        .setVisibleInTemplate()
                        .setDefaultValue(Charsets.UTF_8.name())
                        .setGroup(ZipInput.GROUP_PARAMETERS),
                new RunnerParameter(ZipInput.STOP_AT_FIRST_ERROR, "Stop at first error", Boolean.class, RunnerParameter.Level.OPTIONAL,
                        "Stop at the first error, else continue to process all other files, and the files in error is not in the ZIP")
                        .setVisibleInTemplate()
                        .setDefaultValue(Boolean.TRUE)
                        .setGroup(ZipInput.GROUP_PARAMETERS),
                new RunnerParameter(ZipInput.ZIP_FILENAME, "Zip File Name", String.class, RunnerParameter.Level.REQUIRED,
                        "File Name of the Zip file produced")
                        .setGroup(ZipInput.GROUP_PARAMETERS),
                ZipInput.zipParameterDestinationJsonStorageDefinition
        );

    }

    @Override
    public List<RunnerParameter> getOutputsParameter() {
        return List.of(
                RunnerParameter.getInstance(ZipOutput.ZIP_FILE, "Destination variable name", // label
                        String.class, // class
                        RunnerParameter.Level.REQUIRED, "Process variable where files's reference is saved"));
    }

    @Override
    public Map<String, String> getBpmnErrors() {
        return Map.of(ZipError.WRITE_FILE, ZipError.WRITE_FILE_EXPLANATION,
                ZipError.READ_FILE, ZipError.READ_FILE_EXPLANATION,
                ZipError.RAR_COMPRESSION_NOT_SUPPORTED, ZipError.RAR_COMPRESSION_NOT_SUPPORTED_EXPLANATION,
                ZipError.INCORRECT_STORAGEDEFINITION, ZipError.INCORRECT_STORAGEDEFINITION_EXPLANATION);

    }

    @Override
    public String getSubFunctionName() {
        return "Zip";
    }


    @Override
    public String getSubFunctionDescription() {
        return "Zip a list of documents to one document.";
    }

    @Override
    public String getSubFunctionType() {
        return TYPE_ZIP_ZIP;
    }


}
