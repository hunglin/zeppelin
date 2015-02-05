package com.nflabs.zeppelin.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.nflabs.zeppelin.interpreter.Interpreter;
import com.nflabs.zeppelin.interpreter.InterpreterProperty;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.repl.SparkCommandLine;
import org.apache.spark.repl.SparkILoop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nflabs.zeppelin.interpreter.InterpreterContext;
import com.nflabs.zeppelin.interpreter.InterpreterResult;
import com.nflabs.zeppelin.interpreter.InterpreterResult.Code;
import com.nflabs.zeppelin.notebook.Paragraph;
import scala.Some;
import scala.tools.nsc.Settings;
import scala.tools.nsc.settings.MutableSettings;
import scala.tools.nsc.settings.MutableSettings.PathSetting;


public class SparkInterpreterTest {
	public static SparkInterpreter repl;
  private InterpreterContext context;

	@Before
	public void setUp() throws Exception {
	  if (repl == null) {
		  Properties p = new Properties();

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			SparkILoop sparkILoop = createSparkILoop(out);

	    repl = new SparkInterpreter(p, createSparkContext(sparkILoop), sparkILoop, out);
  	  repl.open();
	  }

    context = new InterpreterContext(new Paragraph(null, null));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBasicIntp() {
		assertEquals(InterpreterResult.Code.SUCCESS, repl.interpret("val a = 1\nval b = 2", context).code());

		// when interpret incomplete expression
		InterpreterResult incomplete = repl.interpret("val a = \"\"\"", context);
		assertEquals(InterpreterResult.Code.INCOMPLETE, incomplete.code());
		assertTrue(incomplete.message().length()>0); // expecting some error message
		/*
		assertEquals(1, repl.getValue("a"));
		assertEquals(2, repl.getValue("b"));
		repl.interpret("val ver = sc.version");
		assertNotNull(repl.getValue("ver"));
		assertEquals("HELLO\n", repl.interpret("println(\"HELLO\")").message());
		*/
	}

	@Test
	public void testEndWithComment() {
		assertEquals(InterpreterResult.Code.SUCCESS, repl.interpret("val c=1\n//comment", context).code());
	}

	@Test
	public void testSparkSql(){
		repl.interpret("case class Person(name:String, age:Int)", context);
		repl.interpret("val people = sc.parallelize(Seq(Person(\"moon\", 33), Person(\"jobs\", 51), Person(\"gates\", 51), Person(\"park\", 34)))", context);
		assertEquals(Code.SUCCESS, repl.interpret("people.take(3)", context).code());

		// create new interpreter
		Properties p = new Properties();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		SparkILoop sparkILoop = createSparkILoop(out);
		SparkInterpreter repl2 =
				new SparkInterpreter(p, createSparkContext(sparkILoop), sparkILoop, out);
		repl2.open();

		repl.interpret("case class Man(name:String, age:Int)", context);
		repl.interpret("val man = sc.parallelize(Seq(Man(\"moon\", 33), Man(\"jobs\", 51), Man(\"gates\", 51), Man(\"park\", 34)))", context);
		assertEquals(Code.SUCCESS, repl.interpret("man.take(3)", context).code());
		repl2.getSparkContext().stop();
	}

	@Test
	public void testReferencingUndefinedVal(){
		InterpreterResult result = repl.interpret("def category(min: Int) = {" +
				       "    if (0 <= value) \"error\"" +
                       "}", context);
		assertEquals(Code.ERROR, result.code());
		System.out.println("msg="+result.message());
	}

	private SparkContext createSparkContext(SparkILoop interpreter) {
		System.err.println("------ in test Create new SparkContext " + getProperty("master") + " -------");

		String execUri = System.getenv("SPARK_EXECUTOR_URI");
		String[] jars = SparkILoop.getAddedJars();
		SparkConf conf =
				new SparkConf()
						.setMaster(getProperty("master"))
						.setAppName(getProperty("spark.app.name"))
						.setJars(jars)
						.set("spark.repl.class.uri", interpreter.intp().classServer().uri());

		if (execUri != null) {
			conf.set("spark.executor.uri", execUri);
		}
		if (System.getenv("SPARK_HOME") != null) {
			conf.setSparkHome(System.getenv("SPARK_HOME"));
		}
		conf.set("spark.scheduler.mode", "FAIR");

		Properties intpProperty = new Properties();

		for (Object k : intpProperty.keySet()) {
			String key = (String) k;
			if (key.startsWith("spark.")) {
				Object value = intpProperty.get(key);
				if (value != null
						&& value instanceof String
						&& !((String) value).trim().isEmpty()) {
					conf.set(key, (String) value);
				}
			}
		}

		SparkContext sparkContext = new SparkContext(conf);
		return sparkContext;
	}

	private SparkILoop createSparkILoop(ByteArrayOutputStream out) {
		SparkILoop sparkILoop = new SparkILoop(null, new PrintWriter(out));
		Settings settings = createSettings();
		sparkILoop.settings_$eq(settings);
		sparkILoop.createInterpreter();
		sparkILoop.loadFiles(settings);
		sparkILoop.intp().setContextClassLoader();
		sparkILoop.intp().initializeSynchronous();

		return sparkILoop;
	}

	private Settings createSettings() {
		Settings settings = new Settings();
//		if (getProperty("args") != null) {
//			String[] argsArray = getProperty("args").split(" ");
//			LinkedList<String> argList = new LinkedList<String>();
//			for (String arg : argsArray) {
//				argList.add(arg);
//			}
//
//			SparkCommandLine command =
//					new SparkCommandLine(scala.collection.JavaConversions.asScalaBuffer(
//							argList).toList());
//			settings = command.settings();
//		}

		LinkedList<String> argList = new LinkedList<>();
		SparkCommandLine command =
				new SparkCommandLine(scala.collection.JavaConversions.asScalaBuffer(
						argList).toList());
		settings = command.settings();

		PathSetting pathSettings = settings.classpath();
		String classpath = "";
		List<File> paths = currentClassPath();
		for (File f : paths) {
			if (classpath.length() > 0) {
				classpath += File.pathSeparator;
			}
			classpath += f.getAbsolutePath();
		}

		/* commented out since urls is null */
//		if (urls != null) {
//			for (URL u : urls) {
//				if (classpath.length() > 0) {
//					classpath += File.pathSeparator;
//				}
//				classpath += u.getFile();
//			}
//		}

		pathSettings.v_$eq(classpath);
		settings.scala$tools$nsc$settings$ScalaSettings$_setter_$classpath_$eq(pathSettings);
		settings.explicitParentLoader_$eq(new Some<ClassLoader>(Thread.currentThread()
				.getContextClassLoader()));
		MutableSettings.BooleanSetting b = (MutableSettings.BooleanSetting) settings.usejavacp();
		b.v_$eq(true);
		settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);

		return settings;
	}

	private List<File> currentClassPath() {
		List<File> paths = classPath(Thread.currentThread().getContextClassLoader());
		String[] cps = System.getProperty("java.class.path").split(File.pathSeparator);
		if (cps != null) {
			for (String cp : cps) {
				paths.add(new File(cp));
			}
		}
		return paths;
	}

	private List<File> classPath(ClassLoader cl) {
		List<File> paths = new LinkedList<File>();
		if (cl == null) {
			return paths;
		}

		if (cl instanceof URLClassLoader) {
			URLClassLoader ucl = (URLClassLoader) cl;
			URL[] urls = ucl.getURLs();
			if (urls != null) {
				for (URL url : urls) {
					paths.add(new File(url.getFile()));
				}
			}
		}
		return paths;
	}


	private String getProperty(String key) {
		Map<String, InterpreterProperty> defaultProperties = Interpreter
				.findRegisteredInterpreterByClassName("com.nflabs.zeppelin.spark.SparkInterpreter")
				.getProperties();
		if (defaultProperties.containsKey(key)) {
			return defaultProperties.get(key).getDefaultValue();
		}

		return null;
	}
}
