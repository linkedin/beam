package org.apache.beam.sdk.options;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;


/**
 * LI-SPECIFIC CHANGE:
 * A helper class for unit tests to set and get properties on a method.
 */
@Experimental
public class UnitTestHelper {
  private static final ThreadLocal<Map<Class, Map<String, Object>>> UNIT_TEST_HELPER_MAP = ThreadLocal.withInitial(HashMap::new);
  public static Boolean containsProperty(Method method, String propertyName) {
    return UNIT_TEST_HELPER_MAP.get().containsKey(method.getDeclaringClass()) &&
     UNIT_TEST_HELPER_MAP.get().get(method.getDeclaringClass()).containsKey(propertyName);
  }

  public static Object getProperty(Method method, String propertyName){
    return UNIT_TEST_HELPER_MAP.get().get(method.getDeclaringClass()).get(propertyName);
  }

  public static void setProperty(Method method, String propertyName, Object value){
    if (!UNIT_TEST_HELPER_MAP.get().containsKey(method.getDeclaringClass())) {
      UNIT_TEST_HELPER_MAP.get().put(method.getDeclaringClass(), new HashMap<>());
    }
    Map<String, Object> pipelineInfo = UNIT_TEST_HELPER_MAP.get().get(method.getDeclaringClass());
    pipelineInfo.put(propertyName, value);
    UNIT_TEST_HELPER_MAP.get().put(method.getDeclaringClass(), pipelineInfo);
  }

  public static void clear() {
    UNIT_TEST_HELPER_MAP.get().clear();
  }

}
