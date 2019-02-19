package net.davidbuccola.commons.guice

import com.google.inject.{Injector, Module}

import scala.collection.JavaConverters._

/**
  * Factory for [[LazyInjector]] instances.
  * <p>
  * A [[LazyInjector]] is an injector that lazily configures itself in distributed processing environments (like
  * Spark, Storm and Flink).
  */
object ScalaLazyGuice {

  /**
    * Creates a [[LazyInjector]] which asks for modules at the time of lazy initialization.
    */
  def createInjector(moduleSupplier: () => Seq[Module]): Injector =
    LazyGuice.createInjector(new SerializableSupplier[java.util.Collection[Module]]() {
      override def get(): java.util.Collection[Module] = moduleSupplier.apply().asJavaCollection
    })
}
