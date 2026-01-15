package io.camunda.zip;

import io.camunda.connector.cherrytemplate.CherryInput;

import io.camunda.connector.zip.ZipInput;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class TestListInput {

    @Test
    void testInput() {
        ZipInput input = new ZipInput();
        List<Map<String, Object>> listInputs = input.getInputParameters();
        assert (listInputs != null);
        // checkExistName(listInputs, ZipInput.FOLDER_NAME, null);


    }

    private void checkExistName(List<Map<String, Object>> listInputs, String name, String group) {
        Optional<Map<String, Object>> input = listInputs.stream().filter(t -> t.get("name").equals(name)).findFirst();
        assert (input.isPresent());
        assert group == null || (input.get().get(CherryInput.PARAMETER_MAP_GROUP_LABEL).equals(group));

    }
}
