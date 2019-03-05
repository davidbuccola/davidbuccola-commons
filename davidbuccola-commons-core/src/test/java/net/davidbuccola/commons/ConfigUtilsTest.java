package net.davidbuccola.commons;

import org.junit.Test;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ConfigUtilsTest {

    @Test
    public void testProperties() {
        Config1 config = ConfigUtils.getConfig("config1.properties", Config1.class).get();
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
        assertThat(config.listValue1.size(), is(equalTo(2)));
        assertThat(config.listValue1.get(0), is(equalTo(10101)));
        assertThat(config.listValue1.get(1), is(equalTo(1010101)));
    }

    @Test
    public void testArgsProperties() {
        Config1 config = ConfigUtils.getConfig(
            new String[]{
                "stringValue1=Foo",
                "integerValue1=101",
                "listValue1[0]=10101",
                "listValue1[1]=1010101"},
            Config1.class).get();
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
        assertThat(config.listValue1.size(), is(equalTo(2)));
        assertThat(config.listValue1.get(0), is(equalTo(10101)));
        assertThat(config.listValue1.get(1), is(equalTo(1010101)));
    }

    @Test
    public void testNesting() {
        UberConfig config = ConfigUtils.getConfig("uberconfig.properties", UberConfig.class).get();
        assertThat(config.config1.stringValue1, is(equalTo("Foo")));
        assertThat(config.config1.integerValue1, is(equalTo(101)));
        assertThat(config.config1.listValue1.size(), is(equalTo(2)));
        assertThat(config.config1.listValue1.get(0), is(equalTo(10101)));
        assertThat(config.config1.listValue1.get(1), is(equalTo(1010101)));
        assertThat(config.config2.stringValue2, is(equalTo("Bar")));
        assertThat(config.config2.integerValue2, is(equalTo(202)));
        assertThat(config.config2.listValue2.size(), is(equalTo(2)));
        assertThat(config.config2.listValue2.get(0), is(equalTo(20202)));
        assertThat(config.config2.listValue2.get(1), is(equalTo(2020202)));
    }

    @Test
    public void testSystemPropertyOverride() {
        System.setProperty("override.integerValue1", "10101");
        try {
            Config1 config = ConfigUtils.getConfig("config1.properties", Config1.class).get();
            assertThat(config.stringValue1, is(equalTo("Foo")));
            assertThat(config.integerValue1, is(equalTo(10101)));
        } finally {
            System.clearProperty("override.integerValue1");
        }
    }

    @Test
    public void testDefaultValue() {
        Config1 config = ConfigUtils.getConfig("config1-empty.properties", Config1.class).get();
        assertThat(config.stringValue1, is(equalTo("defaultString1")));
        assertThat(config.integerValue1, is(equalTo(1)));
    }

    @SuppressWarnings("WeakerAccess")
    public static class Config1 {
        public String stringValue1 = "defaultString1";
        public Integer integerValue1 = 1;
        public List<Integer> listValue1 = emptyList();
    }

    @SuppressWarnings("WeakerAccess")
    public static class Config2 {
        public String stringValue2 = "defaultString2";
        public Integer integerValue2 = 2;
        public List<Integer> listValue2 = emptyList();
    }

    @SuppressWarnings("WeakerAccess")
    public static class UberConfig {
        public Config1 config1 = new Config1();
        public Config2 config2 = new Config2();
    }
}
