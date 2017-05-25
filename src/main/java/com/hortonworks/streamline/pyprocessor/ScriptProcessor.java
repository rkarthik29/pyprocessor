package com.hortonworks.streamline.pyprocessor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

/**
 * Hello world!
 *
 */
public class ScriptProcessor implements CustomProcessorRuntime

{
    private static final String SCRIPT_FILE_LOCATION="ScriptLocation";
    private static final String PARSE_FUNCTION="ParseFunction";
    
    private ScriptEngine engine;
    private String parseFunction;
	
    private static final String SCRIPT_LANGUAGE="ScriptLanguage";
    
    
    private static final String INPUT_DRIVER_ID_KEY = "driverId";
	
	private static final String OUTPUT_FOGGY_WEATHER_ENRICH_KEY="Model_Feature_FoggyWeather";
	private static final String OUTPUT_RAINY_WEATHER_ENRICH_KEY="Model_Feature_RainyWeather";
	private static final String OUTPUT_WINDY_WEATHER_ENRICH_KEY="Model_Feature_WindyWeather";
    
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	public InputStream openInputStream(String streamName) throws IOException {
	    FileSystem fs = FileSystem.get(new Configuration());
	    Path path = new Path(streamName);
	    if(fs.exists(path)) {
	      return fs.open(path);
	    } else {
	      return getClass().getResourceAsStream(streamName);
	    }
	  }

	public void initialize(Map<String, Object> config) {
		// TODO Auto-generated method stub
     engine = new ScriptEngineManager().getEngineByName((String)config.get(SCRIPT_LANGUAGE));
     this.parseFunction=(String)config.get(PARSE_FUNCTION);
try{
	InputStream commonStream = openInputStream((String)config.get(SCRIPT_FILE_LOCATION));
	if (commonStream == null) {
        throw new RuntimeException(
                "Unable to initialize  from either classpath or HDFS");
      }
      engine.eval(new InputStreamReader(commonStream));
}catch(Throwable e){
    throw new RuntimeException(" Script parser Error: ",e);
}
	}

	public List<Result> process(StreamlineEvent event) throws ProcessingException {
		
		Integer driverId = (Integer) event.get(INPUT_DRIVER_ID_KEY);
		
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);
		Invocable invocable = (Invocable) engine;
		try{
			Map<String, Object> enrichedData = (Map<String, Object>)invocable.invokeFunction(this.parseFunction,event);
			builder.putAll(enrichedData);
		List<Result> results = new ArrayList<Result>();
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        //LOG.info("Enriched StreamLine Event with weather is: " + enrichedEvent );
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        results.add(new Result("weather_enrich_stream", newEvents));
        return results;
		
		}catch(Exception e){
			throw new ProcessingException(e.getMessage()+ "you forget to implement a function???");
		}
		

	}

	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub
		
	}
   
}
