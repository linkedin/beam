package org.apache.beam.runners.samza.util;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import org.apache.commons.lang3.tuple.Pair;


public class ExceptionListener {
  private PropertyChangeSupport support;
  private static final ExceptionListener singletonExceptionListener = new ExceptionListener();

  private ExceptionListener() {
    support = new PropertyChangeSupport(this);
  }
  public static ExceptionListener getInstance() {
    return singletonExceptionListener;
  }

  public void addPropertyChangeListener(PropertyChangeListener pcl) {
    support.addPropertyChangeListener(pcl);
  }

  public void removePropertyChangeListener(PropertyChangeListener pcl) {
    support.removePropertyChangeListener(pcl);
  }

  public void setException(Pair<String, Exception> value) {
    support.firePropertyChange("exception", null, value);
  }
}
