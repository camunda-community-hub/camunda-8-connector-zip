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
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
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
            int count=0;
            for (Object file : zipInput.getListSourceFile()) {
                count++;
                FileVariableReference fileVariableReference;
                FileVariable fileVariable=null;
                logExecution.append("Zip ");
                try {
                    fileVariableReference = FileVariableReference.fromObject(file);
                    fileVariable = fileRepoFactory.loadFileVariable(fileVariableReference, context);
                    InputStream inputStreamFile = fileVariable.getValueStream();

                    // ZIP the file
                    String fileName=fileVariableReference.originalFileName;

                    if (fileName== null) {
                        String source=fileVariableReference.getContent().toString();
                        // maybe come from an URL? No file name in that situation
                        fileName = URLDecoder.decode(source.substring(source.lastIndexOf('/') + 1), StandardCharsets.UTF_8);
                        if (fileName == null || fileName.length()>50)
                            fileName = "File "+count;
                    }
                    ZipArchiveEntry zipEntry = new ZipArchiveEntry(fileName);
                    zipOut.putArchiveEntry(zipEntry);
                    inputStreamFile.transferTo(zipOut);

                    logExecution.append(String.format("[%s],",fileName));
                    logger.debug("{}/{} file [{}] ", count,zipInput.getListSourceFile().size(),  fileName);
                    zipOut.closeArchiveEntry();
                } catch(Exception e) {
                    logger.error("Upload error on file {}", fileVariable!=null? fileVariable.getName(): "", e);
                    logExecution.append(String.format("Error on [{}] : {}", fileVariable!=null? fileVariable.getName(): "", e.getMessage()));
                    if (zipInput.getStopAtFirstError()) {
                        throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file [" + zipInput.getSourceFile()
                                + " :" + e.getMessage());
                    }

                }
            }

            zipOut.finish();
            InputStream zipBufferInputStream = new ByteArrayInputStream(zipBufferOutputStream.toByteArray());

            zipOutput.zipFile = ZipToolbox.write(zipInput.getZipFileName(), "application/zip", zipBufferInputStream, zipInput.getStorageDefinitionObject(), fileRepoFactory, context);
            logExecution.append(String.format("], Write [%s]",zipInput.getZipFileName()));

        } catch(ConnectorException ce) {
            throw  ce;
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
                new RunnerParameter(ZipInput.SOURCE_FILE, "Zip Connection", String.class,
                        RunnerParameter.Level.REQUIRED, "Zip Connection. JSON like {\"url\":\"http://localhost:8099/Zip/browser\",\"userName\":\"test\",\"password\":\"test\"}"),
                new RunnerParameter(ZipInput.FILTER_FILE, "Filter File", String.class,
                        RunnerParameter.Level.OPTIONAL, "Recursive name: folder name can contains '/'") //
                        .setVisibleInTemplate() //
                        .setDefaultValue("*"),//
                new RunnerParameter(ZipInput.KEEP_FOLDER_STRUCTURE, "Keep Folder Structure", Boolean.class, RunnerParameter.Level.REQUIRED,
                        "Folder name to be created."));
    }

    @Override
    public List<RunnerParameter> getOutputsParameter() {
        return List.of(
                RunnerParameter.getInstance(ZipOutput.LIST_DOCUMENTS, "Folder ID created", String.class,
                        RunnerParameter.Level.OPTIONAL,
                        "Folder ID created. In case of a recursive creation, ID of the last folder (deeper)"));
    }

    @Override
    public Map<String, String> getBpmnErrors() {
        return Map.of(ZipError.FOLDER_CREATION, ZipError.FOLDER_CREATION_EXPLANATION,
                ZipError.INVALID_PARENT, ZipError.INVALID_PARENT_EXPLANATION);

    }

    @Override
    public String getSubFunctionName() {
        return "Zip";
    }


    @Override
    public String getSubFunctionDescription() {
        return "Unzip a document, and create a list of document in the storage.";
    }

    @Override
    public String getSubFunctionType() {
        return TYPE_ZIP_ZIP;
    }


}
