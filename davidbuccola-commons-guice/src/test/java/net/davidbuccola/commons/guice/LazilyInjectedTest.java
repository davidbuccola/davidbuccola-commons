package net.davidbuccola.commons.guice;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class LazilyInjectedTest {

    private Injector injector;

    @Before
    public void setUp() {
        injector = new LazyInjector(() -> ImmutableSet.of(new AbstractModule() {
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
        }));
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
        LazilyInjected<SimpleBean> simpleBean;

        @Override
        public void run() {
            assertThat("LazilyInjected wrapper should be injected", simpleBean, is(notNullValue()));
            assertThat("Lazily injected value inside LazilyInjected wrapper should not be injected yet", not(simpleBean.isInjected()));

            SimpleBean injectedValue = simpleBean.get();
            assertThat(injectedValue, is(notNullValue()));
            assertThat(injectedValue.getValue(), is(equalTo("0")));
        }
    }

    private static class GenericStringInjectionCase implements Runnable {
        @Inject
        LazilyInjected<GenericBean<String>> stringBean;

        @Override
        public void run() {
            assertThat("LazilyInjected wrapper should be injected", stringBean, is(notNullValue()));
            assertThat("Lazily injected value inside LazilyInjected wrapper should not be injected yet", not(stringBean.isInjected()));

            GenericBean<String> injectedValue = stringBean.get();
            assertThat(injectedValue, is(notNullValue()));
            assertThat(injectedValue.getValue(), is(equalTo("0")));
        }
    }

    private static class GenericIntegerInjectionCase implements Runnable {
        @Inject
        LazilyInjected<GenericBean<Integer>> integerBean;

        @Override
        public void run() {
            assertThat("LazilyInjected wrapper should be injected", integerBean, is(notNullValue()));
            assertThat("Lazily injected value inside LazilyInjected wrapper should not be injected yet", not(integerBean.isInjected()));

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

        String getValue() {
            return value;
        }
    }

    static class GenericBean<T> {
        private final T value;

        @Inject
        GenericBean(T value) {
            this.value = value;
        }

        T getValue() {
            return value;
        }
    }
}
