package org.apache.beam.sdk.testing;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Interface to support handling properties with customized options.
 */
@Experimental
public interface PropertyCustomizationHandler {

  /**
   * Checks if customization is enabled.
   *
   * @return true if customization is enabled, false otherwise.
   */
  Boolean isCustomizationEnabled();

  /**
   * Checks if the specified property is contained within the handler.
   *
   * @param method the method associated with the property.
   * @param propertyName the name of the property.
   * @return true if the property is contained, false otherwise.
   */
  Boolean containsProperty(Method method, String propertyName);

  /**
   * Retrieves the value of the specified property.
   *
   * @param method the method associated with the property.
   * @param propertyName the name of the property.
   * @return the value of the property.
   */
  Object getProperty(Method method, String propertyName);

  /**
   * Sets the value of the specified property.
   *
   * @param method the method associated with the property.
   * @param propertyName the name of the property.
   * @param value the value to set.
   */
  void setProperty(Method method, String propertyName, Object value);

  /**
   * Clears all properties.
   */
  void clear();

  /**
   * Registrar interface for creating instances of PropertyCustomizationHandler.
   */
  interface Registrar {
    PropertyCustomizationHandler create();
  }

  /**
   * Retrieves an instance of PropertyCustomizationHandler using the ServiceLoader.
   *
   * @return an instance of PropertyCustomizationHandler, or null if none is found.
   */
  static @Initialized @Nullable PropertyCustomizationHandler get() {
    final Iterator<PropertyCustomizationHandler.Registrar> initializer =
        ServiceLoader.load(PropertyCustomizationHandler.Registrar.class).iterator();
    return initializer.hasNext() ? Iterators.getOnlyElement(initializer).create() : null;
  }
}