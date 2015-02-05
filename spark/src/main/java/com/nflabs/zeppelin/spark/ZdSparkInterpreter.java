package com.nflabs.zeppelin.spark;

import com.nflabs.zeppelin.interpreter.Interpreter;
import com.nflabs.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Spark Interpreter that allows injection of spark context
 */
public class ZdSparkInterpreter extends SparkInterpreter {

  Logger logger = LoggerFactory.getLogger(ZdSparkInterpreter.class);

  static {
    Interpreter.register(
            "zdspark",
            "zdspark",
            ZdSparkInterpreter.class.getName(),
            new InterpreterPropertyBuilder()
                    .add("args", "", "spark commandline args").build());

  }

  public ZdSparkInterpreter(Properties property) {
    super(property);
  }

  /**
   * Set the spark context of the interpreter
   */
  public void setSparkContext(SparkContext sc) {
    //TODO(hunglin): need to change the accessibility of SparkInterpreter.sc to protected.
  }
}
