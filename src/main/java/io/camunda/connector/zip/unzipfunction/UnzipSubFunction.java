/* ******************************************************************** */
/*                                                                      */
/*  CreateFolderWorker                                                   */
/*                                                                      */
/*  Create a folder in CMIS      */
/* ******************************************************************** */
package io.camunda.connector.zip.unzipfunction;

import com.github.junrar.Archive;
import com.github.junrar.rarfile.FileHeader;
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
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class UnzipSubFunction implements ZipSubFunction {
    public static final String TYPE_ZIP_UNZIP = "unzip";
    private final Logger logger = LoggerFactory.getLogger(UnzipSubFunction.class.getName());

    public UnzipSubFunction() {
        // No special to add here
    }

    @Override
    public ZipOutput executeSubFunction(ZipInput zipInput,
                                        OutboundConnectorContext context, StringBuilder logExecution) throws ConnectorException {
        ZipOutput zipOutput = new ZipOutput();
        FileRepoFactory fileRepoFactory = FileRepoFactory.getInstance();
        FileVariable fileVariable = null;
        ZipInput.CompressFormat compressFormat = null;
        FileVariableReference fileVariableReference;
        try {
            fileVariableReference = FileVariableReference.fromObject(zipInput.getSourceFile());

            fileVariable = fileRepoFactory.loadFileVariable(fileVariableReference, context);
            compressFormat = detectFormat(fileVariable.getValueStream());
        } catch (Exception e) {
            logger.error("Upload error on file {}", zipInput.getSourceFile(), e);
            throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file [" + zipInput.getSourceFile()
                    + " :" + e.getMessage());
        }
        logger.debug("Unzipping file [{}] compressFormat[{}]", fileVariable.getName(), compressFormat.toString());
        logExecution.append(String.format("Unzipping file [%s] compressFormat[%s];", fileVariable.getName(), compressFormat));

        try {
            if (compressFormat == ZipInput.CompressFormat.ZIP) {
                zipOutput.listDocumentsId = unzipOperation(fileVariableReference, fileVariable.getName(), zipInput, fileRepoFactory, context, logExecution);
            } else if (compressFormat == ZipInput.CompressFormat.RAR) {
                fileVariable = fileRepoFactory.loadFileVariable(fileVariableReference, context);
                zipOutput.listDocumentsId = unRarOoperation(fileVariable.getValueStream(), fileVariable.getName(), zipInput, fileRepoFactory, context, logExecution);
            }

            //----- Reorder the output
            zipOutput.listDocumentsId = reorder(zipInput.getSortZipEntry(), zipOutput.listDocumentsId);
            String logDocumentsId = zipOutput.listDocumentsId.stream()
                    .map(f -> f.originalFileName)
                    .collect(Collectors.joining(", "));

            logExecution.append(String.format("[%s] output documents", logDocumentsId));
            return zipOutput;
        } catch (ConnectorException ce) {
            throw ce;
        } catch (Exception e) {
            logger.error("Management error", e);
            throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file :" + e.getMessage());
        }


    }

    @Override
    public List<RunnerParameter> getInputsParameter() {
        return List.of(
                new RunnerParameter(ZipInput.SOURCE_FILE, "Source file to unzip", String.class,
                        RunnerParameter.Level.REQUIRED,
                        "Source file to unzip"));
    }

    @Override
    public List<RunnerParameter> getOutputsParameter() {
        return List.of(
                RunnerParameter.getInstance(ZipOutput.LIST_DOCUMENTS, "List od documents unzipped", String.class,
                        RunnerParameter.Level.OPTIONAL,
                        "Folder ID created. In case of a recursive creation, ID of the last folder (deeper)"));
    }

    @Override
    public Map<String, String> getBpmnErrors() {
        return Map.of(ZipError.FOLDER_CREATION, ZipError.FOLDER_CREATION_EXPLANATION,
                ZipError.INVALID_PARENT, ZipError.INVALID_PARENT_EXPLANATION,
                ZipError.INVALID_COMPRESSION, ZipError.INVALID_COMPRESSION_EXPLANATION,
                ZipError.READ_FILE, ZipError.READ_FILE_EXPLANATION);

    }

    @Override
    public String getSubFunctionName() {
        return "CreateFolder";
    }


    @Override
    public String getSubFunctionDescription() {
        return "Unzip a document, and create a list of document in the storage.";
    }

    @Override
    public String getSubFunctionType() {
        return TYPE_ZIP_UNZIP;
    }

    List<String> decomposeName(String name) {
        return Arrays.stream(name.split("/"))
                .filter(part -> !part.isEmpty())
                .collect(Collectors.toList());
    }




    /* ******************************************************************** */
    /*                                                                      */
    /*  Zip function                                                        */
    /*                                                                      */
    /* ******************************************************************** */

    /**
     * Detect the format
     *
     * @param inputStream input from which the format is detected
     * @return the detected format
     * @throws IOException in case of any error
     */
    public static ZipInput.CompressFormat detectFormat(InputStream inputStream) throws ConnectException {
        try {
            byte[] header = new byte[8];
            int bytesRead = inputStream.read(header);
            if (bytesRead < 4) return null;

            // ZIP signature: 50 4B 03 04
            if (header[0] == 0x50 && header[1] == 0x4B) {
                return ZipInput.CompressFormat.ZIP;
            }

            // RAR 4.x or 5.x signature: 52 61 72 21 1A 07 00/01
            if (header[0] == 0x52 && header[1] == 0x61 &&
                    header[2] == 0x72 && header[3] == 0x21 &&
                    header[4] == 0x1A && header[5] == 0x07) {
                return ZipInput.CompressFormat.RAR;
            }
            String message = "Invalid header characters "
                    + String.valueOf(header[0])
                    + "]["
                    + String.valueOf(header[1])
                    + "]";
            throw new ConnectorException(ZipError.INVALID_COMPRESSION,
                    message);

        } catch (Exception e) {
            throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file :" + e.getMessage());

        }
    }


    private static final List<Charset> ZIP_CHARSETS = List.of(
            StandardCharsets.UTF_8,
            Charset.forName("CP437"),
            Charset.forName("windows-1252")
    );

    /**
     * ManageZip
     *
     * @param fileVariableReference Source file to unzip
     * @param zipFileName           Zip filename
     * @param zipInput              input of all function
     * @param fileRepoFactory       fileRepoFactory
     * @param context               context of connector
     * @param logExecution          logExecution
     * @return the lisf of file unzip
     */
    private List<FileVariableReference> unzipOperation(FileVariableReference fileVariableReference,
                                                       String zipFileName,
                                                       ZipInput zipInput,
                                                       FileRepoFactory fileRepoFactory,
                                                       OutboundConnectorContext context,
                                                       StringBuilder logExecution) {
        StorageDefinition storageOutputDefinition = zipInput.getStorageDefinitionObject();
        for (Charset charset : ZIP_CHARSETS) {
            int filesFiltered = 0;
            List<FileVariableReference> listDocumentsId = new ArrayList<>();
            // Start the inputStream now
            InputStream inputStream = null;
            try {
                FileVariable fileVariable = fileRepoFactory.loadFileVariable(fileVariableReference, context);
                inputStream = fileVariable.getValueStream();
            } catch (Exception e) {
                logger.error("Upload error on file {}", zipInput.getSourceFile(), e);
                throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file [" + zipInput.getSourceFile()
                        + " :" + e.getMessage());
            }

            String entryFileName = null;
            try {
                ZipArchiveInputStream zis = new ZipArchiveInputStream(inputStream, charset.name(), true);

                ZipArchiveEntry entry;
                while ((entry = zis.getNextZipEntry()) != null) {
                    if (entry.isDirectory())
                        continue;

                    entryFileName = entry.getName();
                    FilterStatus filterStatus = filterByName(zipFileName, zipInput, entry.getName());
                    if (filterStatus.filter) {
                        filesFiltered++;
                        continue;
                    }
                    Path temp = Paths.get(entry.getName());
                    String mimeType = Files.probeContentType(temp);

                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    zis.transferTo(buffer);
                    ByteArrayInputStream entryStream = new ByteArrayInputStream(buffer.toByteArray());

                    listDocumentsId.add(ZipToolbox.write(filterStatus.fileName, mimeType, entryStream, storageOutputDefinition, fileRepoFactory, context));

                }
                logger.debug("ZIP File [{}] uncompressed {} files with success charset[{}] filesFiltered: {} ", zipFileName, listDocumentsId.size(), charset.name(), filesFiltered);
                logExecution.append(String.format("ZIP File [%s] uncompressed %d files with success charset[%s] filesFiltered: %d;", zipFileName, listDocumentsId.size(), charset.name(), filesFiltered));

                return listDocumentsId;
            } catch (Exception e) {

                // if the error is because of the chartset, we can continue
                if (e instanceof InvalidPathException || e.getMessage().contains("invalid LOC header")) {
                    logger.info("Unzip file [{}] Entry [{}] Charset [{}] got an error [{}], use the next Chartset in the list", zipFileName, entryFileName, charset.name(), e.getMessage());

                } else {
                    logger.error("Upload error file [{}] entryFileName[{}]", zipFileName, entryFileName, e);
                    throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file [" + zipFileName + "] Entry[" + entryFileName + "] :" + e.getMessage());
                }
            }
        }
        logger.error("Can't read: all charset failed", zipFileName);
        throw new ConnectorException(ZipError.READ_FILE, "All chartset failed with file [" + zipFileName + "]");
    }


    /**
     * ManageRarfile
     *
     * @param inputStream     inputStream for tghe Rar file
     * @param zipFileName     original file name
     * @param zipInput        Input to access any other parameter
     * @param fileRepoFactory fileRepoFactory
     * @param context         context
     * @return list of variables created
     */
    private List<FileVariableReference> unRarOoperation(InputStream inputStream,
                                                        String zipFileName,
                                                        ZipInput zipInput,
                                                        FileRepoFactory fileRepoFactory,
                                                        OutboundConnectorContext context,
                                                        StringBuilder logExecution) {
        List<FileVariableReference> listDocumentsId = new ArrayList<>();
        StorageDefinition storageOutputDefinition = zipInput.getStorageDefinitionObject();
        int filesFiltered = 0;
        try (Archive archive = new Archive(inputStream)) {
            FileHeader fileHeader;
            while ((fileHeader = archive.nextFileHeader()) != null) {
                String completeFileName = fileHeader.getFileNameString();
                String fileName = Paths.get(completeFileName).getFileName().toString();

                FilterStatus filterStatus = filterByName(zipFileName, zipInput, fileName);
                if (filterStatus.filter) {
                    filesFiltered++;
                    continue;
                }

                FileVariable fileVariableOutput = new FileVariable();
                fileVariableOutput.setStorageDefinition(storageOutputDefinition);
                fileVariableOutput.setName(filterStatus.fileName);

                Path temp = Paths.get(fileName);
                fileVariableOutput.setMimeType(Files.probeContentType(temp));

                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                archive.extractFile(fileHeader, buffer);

                ByteArrayInputStream entryStream = new ByteArrayInputStream(buffer.toByteArray());
                fileVariableOutput.setValueStream(entryStream);

                // ------- Write it now
                try {
                    FileVariableReference fileVariableOutputReference = fileRepoFactory.saveFileVariable(fileVariableOutput, context);
                    listDocumentsId.add(fileVariableOutputReference);
                } catch (Exception e) {
                    throw new ConnectorException(ZipError.WRITE_FILE,
                            "Error during write file to the output storage " + e.getMessage());
                }
            }

            logger.debug("RAR File [{}] uncompressed {} files with success filesFiltered: {}", zipFileName, listDocumentsId.size(), filesFiltered);
            logExecution.append(String.format("RAR File [%s] uncompressed %d files with success filesFiltered: %d;", zipFileName, listDocumentsId.size(), filesFiltered));

        } catch (Exception e) {
            logger.error("Upload error file [{}]", zipFileName, e);
            throw new ConnectorException(ZipError.READ_FILE, "Can't Read the file [" + zipFileName + "] :" + e.getMessage());
        }
        return listDocumentsId;
    }


    /* ******************************************************************** */
    /*                                                                      */
    /*  Common function                                                     */
    /*                                                                      */
    /* ******************************************************************** */
    private List<FileVariableReference> reorder(ZipInput.SORTENTRY sortEntry, List<FileVariableReference> listDocumentsId) {
        String logOrder = listDocumentsId.stream()
                .map(f -> f.originalFileName)
                .collect(Collectors.joining(", "));
        logger.debug("Before reorder [{}]", logOrder);

        switch (sortEntry) {
            case NONE -> {
            }
            case NUMBER -> {
                listDocumentsId.sort(Comparator.comparingDouble(e -> {
                    return extractNumber(e.originalFileName, Double.MAX_VALUE);
                }));
            }
            case NUMBERASCII -> {
                listDocumentsId.sort((a, b) ->
                {
                    int returnValue;
                    String log;
                    Double aDouble = extractNumber(a.originalFileName, null);
                    Double bDouble = extractNumber(b.originalFileName, null);
                    if (aDouble != null && bDouble != null) {
                        log = "doubleCompare";
                        returnValue = aDouble.compareTo(bDouble);
                    } else if (aDouble == null && bDouble == null) {
                        log = "asciiCompare";
                        returnValue = a.originalFileName.compareToIgnoreCase(b.originalFileName);
                    } else {// Ok, so here one is a number, second is not a number.
                        String strName = aDouble == null ? a.originalFileName : b.originalFileName;
                        boolean stringIsInferior = (strName.isEmpty() || strName.trim().startsWith("_"));

                        // consider any other string are upper

                        if (aDouble == null) {
                            log = "AAscii (" + stringIsInferior + ")/bDouble (" + bDouble + ")";
                            returnValue = stringIsInferior ? -1 : 1;
                        } else {
                            log = "ADouble(" + aDouble + ")/BAscii(" + stringIsInferior + ")";
                            returnValue = stringIsInferior ? 1 : -1;
                        }
                    }
                    logger.debug("[{}] <=> [{}] : {} - {}", a.originalFileName, b.originalFileName, returnValue, log);
                    return returnValue;
                });
            }

            case ASCII -> {
                listDocumentsId.sort((a, b) ->
                        a.originalFileName.compareToIgnoreCase(b.originalFileName));

            }
        }

        logOrder = listDocumentsId.stream()
                .map(f -> f.originalFileName)
                .collect(Collectors.joining(", "));

        logger.debug("After reorder [{}]", logOrder);

        return listDocumentsId;
    }



    /* ******************************************************************** */
    /*                                                                      */
    /*  Function                                                             */
    /*                                                                      */
    /* ******************************************************************** */

    public record FilterStatus(boolean filter, String fileName) {
    }


    /**
     * @param zipFileName file name to verify
     * @param zipInput to access all filters information
     * @param name name
     * @return the filterStatus
     */
    private FilterStatus filterByName(String zipFileName, ZipInput zipInput, String name) {
        // Filter?
        List<String> decompose = decomposeName(name);
        String entryFileName = decompose.get(decompose.size() - 1);
        if (zipInput.filterFile != null && !zipInput.filterFile.trim().isEmpty()) {
            // Transform the filter in a regular regexp. User says "a*.jpeg" when the regular expression is "a.*\\.jpeg
            // * must be translated to .* and a . is \\.
            String regex = zipInput.filterFile.replace(".", "\\.")    // escape literal dots
                    .replaceAll("(?<!\\()\\?", ".")      // replace ? unless preceded by (
                    .replace("*", ".*");     // expand wildcard *
            if (!entryFileName.matches(regex)) {
                logger.info("File [{}] Skipping file [{}] does not math filterFile[{}]", zipFileName, entryFileName, regex);
                return new FilterStatus(true, entryFileName);
            }
        }
        if (zipInput.filterPath != null && !zipInput.filterPath.trim().isEmpty()) {
            // Transform the filter in a regular regexp. User says "/src/java/*" when the regular expression is "/src/java/.*
            // * must be translated to .* and a . is \\.
            String regex = zipInput.filterPath.replace(".", "\\.")    // escape literal dots
                    .replace("?", ".")     // one character
                    .replace("*", ".*");     // expand wildcard *
            if (!name.matches(regex)) {
                logger.info("File [{}] Skipping file [{}] does not math filterPath[{}]", zipFileName, entryFileName, regex);
                return new FilterStatus(true, entryFileName);
            }
        }
        return new FilterStatus(false, entryFileName);
    }


    /**
     * Extract number
     *
     * @param name name to use
     * @param defaultValue if no number can be extracted
     * @return the value (which may be the default value)
     */
    public static Double extractNumber(String name, Double defaultValue) {
        String lower = name.toLowerCase();

        // 1️⃣ Extract number (integer or decimal)
        Matcher m = Pattern.compile("(\\d+(?:\\.\\d+)?)").matcher(lower);
        if (!m.find()) {
            return defaultValue; // no number found
        }

        double value = Double.parseDouble(m.group(1));

        // 2️⃣ Apply suffix rules
        if (lower.contains("bis")) {
            return value + 0.2;
        }
        if (lower.contains("tier")) {
            return value + 0.3;
        }

        return value;
    }
}