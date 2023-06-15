package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CustomHeaderOptionsTest {

    @Test
    public void get_custom_attributes_test() {
        assertNull(new CustomHeaderOptions().getCustomAttributes());
    }

    @Test
    public void get_target_model_test() {
        assertNull(new CustomHeaderOptions().getTargetModel());
    }

    @Test
    public void get_target_variant_test() {
        assertNull(new CustomHeaderOptions().getTargetVariant());
    }

    @Test
    public void get_target_container_hostname_test() {
        assertNull(new CustomHeaderOptions().getTargetContainerHostname());
    }

    @Test
    public void get_inference_id_test() {
        assertNull(new CustomHeaderOptions().getInferenceId());
    }

    @Test
    public void get_enable_explanations_test() {
        assertNull(new CustomHeaderOptions().getEnableExplanations());
    }


}
