package com.nflabs.zeppelin.spark;

import com.nflabs.zeppelin.interpreter.Interpreter;
import com.nflabs.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Spark SQL interpreter uses ZdSparkInterpreter
 */
public class ZdSparkSqlInterpreter extends SparkSqlInterpreter {
  Logger logger = LoggerFactory.getLogger(ZdSparkSqlInterpreter.class);

  static {
    Interpreter.register(
        "zdsql",
        "zdspark",
        ZdSparkSqlInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add("zeppelin.spark.maxResult", "10000", "Max number of SparkSQL result to display")
            .build());
  }

  public ZdSparkSqlInterpreter(Properties property) {
    super(property);
  }
}
