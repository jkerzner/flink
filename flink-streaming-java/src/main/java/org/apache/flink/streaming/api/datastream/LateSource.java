package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


public class LateSource<T> implements SourceFunction, ResultTypeQueryable, StoppableFunction {

	// shared multiplexer for incoming elements so they get passed to the correct streams
	private static Map<String, List> sharedBuffer = Collections.synchronizedMap(new HashMap<String, List>());

	// each actual LateSource has a unique identifier based the elements' original source
	private String identifier;

	private transient volatile boolean running = true;
	Logger LOG = LoggerFactory.getLogger(LateSource.class);

	public LateSource(String id) {
		this.identifier = id;

		// register the new LateSource in the buffer so we have a place to receive elements
		sharedBuffer.put(id, Collections.synchronizedList(new LinkedList<String>()));
	}

	@Override
	public void run(SourceContext ctx) throws Exception {
		running = true;

		while(running) {

			// check to see if our buffer in the hashmap has anything waiting for us to consume
			if(sharedBuffer.get(this.identifier).size() > 0) {
				synchronized (ctx) {
					ctx.collect(this.identifier + " ===> " + (String) sharedBuffer.get(this.identifier).remove(0));
				}
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	public void capture(T e, String eId) {

		// do not process this element if it's not bound for this thread
		if (! eId.equals(this.identifier)) { return; }

		// add the element into the appropriate queue
		sharedBuffer.get(eId).add(e);

		LOG.warn("IIIIIIIIIIIIIIIIIIIIIIIIIII captured into the shared buffer.");
		return;
	}

	@Override
	public TypeInformation getProducedType() {
		return TypeInformation.of(new TypeHint<String>(){});
	}

	@Override
	public void stop() {
		running = false;
	}

	public String getThreadName() {
		return Thread.currentThread().getName();
	}

	public String getIdentifier() {
		return this.identifier;
	}
}
