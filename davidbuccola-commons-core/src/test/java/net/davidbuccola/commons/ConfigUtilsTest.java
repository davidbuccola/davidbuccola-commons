package net.davidbuccola.commons;

import io.dropwizard.configuration.ConfigurationParsingException;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class ConfigUtilsTest {

    @Test
    public void testProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config1.properties"));
        Config1 config = ConfigUtils.buildConfig(properties, Config1.class);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testPropertiesResource() {
        Config1 config = ConfigUtils.buildConfig("config1.properties", Config1.class);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testYamlResource() {
        Config1 config = ConfigUtils.buildConfig("config1.yaml", Config1.class);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testPropertiesWithUnknownFailure() throws IOException {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config1-with-unknown.properties"));
        try {
            ConfigUtils.buildConfig(properties, Config1.class);
            fail("Didn't get expected exception");

        } catch (RuntimeException e) {
            assertThat(e.getCause(), instanceOf(ConfigurationParsingException.class));
        }
    }

    @Test
    public void testPropertiesResourceWithUnknownFailure() {
        try {
            ConfigUtils.buildConfig("config1-with-unknown.properties", Config1.class);
            fail("Didn't get expected exception");

        } catch (RuntimeException e) {
            assertThat(e.getCause(), instanceOf(ConfigurationParsingException.class));
        }
    }

    @Test
    public void testYamlResourceWithUnknownFailure() {
        try {
            ConfigUtils.buildConfig("config1-with-unknown.yaml", Config1.class);
            fail("Didn't get expected exception");

        } catch (RuntimeException e) {
            assertThat(e.getCause(), instanceOf(ConfigurationParsingException.class));
        }
    }

    @Test
    public void testPropertiesWithUnknownAllowed() throws IOException {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config1-with-unknown.properties"));
        Config1 config = ConfigUtils.buildConfig(properties, Config1.class, false);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testPropertiesResourceWithUnknownAllowed() {
        Config1 config = ConfigUtils.buildConfig("config1-with-unknown.properties", Config1.class, false);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testYamlResourceWithUnknownAllowed() {
        Config1 config = ConfigUtils.buildConfig("config1-with-unknown.yaml", Config1.class, false);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testPropertiesNesting() throws IOException {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("uberconfig.properties"));
        UberConfig config = ConfigUtils.buildConfig(properties, UberConfig.class);
        assertThat(config.config1.stringValue1, is(equalTo("Foo")));
        assertThat(config.config1.integerValue1, is(equalTo(101)));
        assertThat(config.config2.stringValue2, is(equalTo("Bar")));
        assertThat(config.config2.integerValue2, is(equalTo(202)));
    }

    @Test
    public void testPropertiesResourceNesting() {
        UberConfig config = ConfigUtils.buildConfig("uberconfig.properties", UberConfig.class);
        assertThat(config.config1.stringValue1, is(equalTo("Foo")));
        assertThat(config.config1.integerValue1, is(equalTo(101)));
        assertThat(config.config2.stringValue2, is(equalTo("Bar")));
        assertThat(config.config2.integerValue2, is(equalTo(202)));
    }

    @Test
    public void testYamlResourceNesting() {
        UberConfig config = ConfigUtils.buildConfig("uberconfig.yaml", UberConfig.class);
        assertThat(config.config1.stringValue1, is(equalTo("Foo")));
        assertThat(config.config1.integerValue1, is(equalTo(101)));
        assertThat(config.config2.stringValue2, is(equalTo("Bar")));
        assertThat(config.config2.integerValue2, is(equalTo(202)));
    }

    @Test
    public void testConfigArg() {
        Config1 config = ConfigUtils.buildConfig(new String[]{"--config=config1.properties"}, Config1.class);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testInlinePropertyArgs() {
        Config1 config = ConfigUtils.buildConfig(new String[]{"stringValue1=Foo", "integerValue1=101"}, Config1.class);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testInlinePropertyArgsIgnoresUnknown() {
        Config1 config = ConfigUtils.buildConfig(new String[]{"stringValue1=Foo", "integerValue1=101", "unknownValue1=Nothing"}, Config1.class);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testSystemPropertyOverride() {
        System.setProperty("override.integerValue1", "10101");
        try {
            Config1 config = ConfigUtils.buildConfig("config1.properties", Config1.class);
            assertThat(config.stringValue1, is(equalTo("Foo")));
            assertThat(config.integerValue1, is(equalTo(10101)));
        } finally {
            System.clearProperty("override.integerValue1");
        }
    }

    @Test
    public void testDefaultValue() {
        Config1 config = ConfigUtils.buildConfig("config1-empty.properties", Config1.class);
        assertThat(config.stringValue1, is(equalTo("defaultString1")));
        assertThat(config.integerValue1, is(equalTo(1)));
    }

    public static class Config1 {
        public String stringValue1 = "defaultString1";
        public Integer integerValue1 = 1;
    }

    public static class Config2 {
        public String stringValue2 = "defaultString2";
        public Integer integerValue2 = 2;
    }

    public static class UberConfig {
        public Config1 config1 = new Config1();
        public Config2 config2 = new Config2();
    }
}
