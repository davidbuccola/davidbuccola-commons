package net.davidbuccola.commons.guice;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class LazyInjectionTest {

    private Injector injector;

    @Before
    public void setUp() {
        injector = new LazyInjector() {
            protected Collection<Module> getModules() {
                return ImmutableSet.of(new AbstractModule() {
                    private Integer testInteger = 0;

                    @Override
                    protected void configure() {
                    }

                    @Provides
                    public Integer getTestInteger() {
                        return testInteger++;
                    }

                    @Provides
                    String getTestString() {
                        return String.valueOf(getTestInteger());
                    }
                });
            }
        };
    }

    @Test
    public void testSimpleInjection() {
        injector.getInstance(SimpleInjectionCase.class).run();
    }

    @Test
    public void testGenericStringInjection() {
        injector.getInstance(GenericStringInjectionCase.class).run();
    }

    @Test
    public void testGenericIntegerInjection() {
        injector.getInstance(GenericIntegerInjectionCase.class).run();
    }

    private static class SimpleInjectionCase implements Runnable {
        @Inject
        LazyInjection<SimpleBean> simpleBean;

        @Override
        public void run() {
            assertThat("LazyInjection wrapper should be injected", simpleBean, is(notNullValue()));
            assertThat("Lazily injected value inside LazyInjection wrapper should not be injected yet", not(simpleBean.isInjected()));

            SimpleBean injectedValue = simpleBean.get();
            assertThat(injectedValue, is(notNullValue()));
        }
    }

    private static class GenericStringInjectionCase implements Runnable {
        @Inject
        LazyInjection<GenericBean<String>> stringBean;

        @Override
        public void run() {
            assertThat("LazyInjection wrapper should be injected", stringBean, is(notNullValue()));
            assertThat("Lazily injected value inside LazyInjection wrapper should not be injected yet", not(stringBean.isInjected()));

            GenericBean<String> injectedValue = stringBean.get();
            assertThat(injectedValue, is(notNullValue()));
            assertThat(injectedValue.getValue(), is(equalTo("0")));
        }
    }

    private static class GenericIntegerInjectionCase implements Runnable {
        @Inject
        LazyInjection<GenericBean<Integer>> integerBean;

        @Override
        public void run() {
            assertThat("LazyInjection wrapper should be injected", integerBean, is(notNullValue()));
            assertThat("Lazily injected value inside LazyInjection wrapper should not be injected yet", not(integerBean.isInjected()));

            GenericBean<Integer> injectedValue = integerBean.get();
            assertThat(injectedValue, is(notNullValue()));
            assertThat(injectedValue.getValue(), is(equalTo(0)));
        }
    }

    static class SimpleBean {
        private final String value;

        @Inject
        SimpleBean(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    static class GenericBean<T> {
        private final T value;

        @Inject
        GenericBean(T value) {
            this.value = value;
        }

        public T getValue() {
            return value;
        }
    }
}
