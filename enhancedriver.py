import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;


def enrich(StreamlineEvent event):
	driverId = int(event.get(INPUT_DRIVER_ID_KEY))
        return enrichedWeather(driverId)


def enrichedWeather(driverId):
	data={}
        data['Model_Feature_FoggyWeather']=1
        data['Model_Feature_RainyWeather']=1
	data['Model_Feature_WindyWeather']=0
