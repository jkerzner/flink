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

public class LateSource<T> implements SourceFunction, ResultTypeQueryable, StoppableFunction {

	private static List list = Collections.synchronizedList(new LinkedList<String>());
	private String toggle;
	private transient volatile boolean running = true;
	private transient Object listLock = new Object();

	Logger LOG = LoggerFactory.getLogger(LateSource.class);

	public LateSource() {

	}

	@Override
	public void run(SourceContext ctx) throws Exception {
		running = true;

		while(running) {
			synchronized(list) {
				synchronized (ctx.getCheckpointLock()) {
					if (list.size() > 0) {
						//String output = "UMOOOOOOOOOOO " + (String) list.remove(0);
						ctx.collect((String) list.remove(0));
					}
					//ctx.collect(new String(Thread.currentThread().getName()));
				}
			}
		}

	}

	@Override
	public void cancel() {
		running = false;
	}

	public void capture(T e) {
		synchronized (list) {
			list.add(e);
		}

		LOG.warn("PPPPPPPPPPPPPPPP Appended " + list.size() + list.toString() + Thread.currentThread().getName());
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
}
