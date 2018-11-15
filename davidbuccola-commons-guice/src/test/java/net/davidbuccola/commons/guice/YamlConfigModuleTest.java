package net.davidbuccola.commons.guice;

import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class YamlConfigModuleTest {

    @Test
    public void testConfig() {
        String configString = "stringValue1: 'Foo'\nintegerValue1: 101";
        Injector injector = Guice.createInjector(new TestModule<>(configString, Config1.class));

        Config1 config = injector.getInstance(Config1.class);
        assertThat(config.stringValue1, is(equalTo("Foo")));
        assertThat(config.integerValue1, is(equalTo(101)));
    }

    @Test
    public void testDefaultValue() {
        String configString = "";
        Injector injector = Guice.createInjector(new TestModule<>(configString, Config1.class));

        Config1 config = injector.getInstance(Config1.class);
        assertThat(config.stringValue1, is(equalTo("defaultString1")));
        assertThat(config.integerValue1, is(equalTo(1)));
    }

    @Test(expected = CreationException.class)
    public void testOneInjectorTwoConfigs() {
        String config1String = "stringValue1: 'Foo'\nintegerValue1: 101";
        String config2String = "stringValue2: 'Bar'\nintegerValue2: 202";

        Guice.createInjector(
            new TestModule<>(config1String, Config1.class),
            new TestModule<>(config2String, Config2.class));
    }

    @Test
    public void testTwoInjectorsTwoConfigs() {
        String config1String = "stringValue1: 'Foo'\nintegerValue1: 101";
        String config2String = "stringValue2: 'Bar'\nintegerValue2: 202";

        Injector injector1 = Guice.createInjector(new TestModule<>(config1String, Config1.class));
        Injector injector2 = Guice.createInjector(new TestModule<>(config2String, Config2.class));

        Config1 config1 = injector1.getInstance(Config1.class);
        assertThat(config1.stringValue1, is(equalTo("Foo")));
        assertThat(config1.integerValue1, is(equalTo(101)));

        Config2 config2 = injector2.getInstance(Config2.class);
        assertThat(config2.stringValue2, is(equalTo("Bar")));
        assertThat(config2.integerValue2, is(equalTo(202)));
    }

    public static class Config1 {
        public String stringValue1 = "defaultString1";
        public Integer integerValue1 = 1;
    }

    public static class Config2 {
        public String stringValue2 = "defaultString2";
        public Integer integerValue2 = 2;
    }

    private static final class TestModule<C> extends YamlConfigModule {

        private TestModule(String configString, Class<C> configClass) {
            super(configString, configClass);
        }
    }
}
