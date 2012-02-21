package etu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.CustomDfsSink;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.flume.handlers.text.output.RawOutputFormat;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

public class BlockDfsSink extends EventSink.Base {
	static final Logger LOG = LoggerFactory.getLogger(BlockDfsSink.class);
	final String path;
	OutputFormat format;

	private final String END = "YES";
	CustomDfsSink writer = null;
	final Map<String, CustomDfsSink> sfWriters = new HashMap<String, CustomDfsSink>();

	boolean shouldSub = false;
	private String filename = "";
	protected String absolutePath = "";

	public BlockDfsSink(String path, String filename, OutputFormat o) {
		this.path = path;
		this.filename = filename;
		shouldSub = Event.containsTag(path) || Event.containsTag(filename);
		this.format = o;
		absolutePath = path;
		if (filename != null && filename.length() > 0) {
			if (!absolutePath.endsWith(Path.SEPARATOR)) {
				absolutePath += Path.SEPARATOR;
			}
			absolutePath += this.filename;
		}
	}

	@SuppressWarnings("deprecation")
	static protected OutputFormat getDefaultOutputFormat() {
		try {
			return FormatFactory.get().getOutputFormat(
					FlumeConfiguration.get().getDefaultOutputFormat());
		} catch (FlumeSpecException e) {
			LOG.warn("format from conf file not found, using default", e);
			return new RawOutputFormat();
		}
	}

	public BlockDfsSink(String path, String filename) {
		this(path, filename, getDefaultOutputFormat());
	}

	protected CustomDfsSink openWriter(String p) throws IOException {
		LOG.info("Opening " + p);
		CustomDfsSink w = new CustomDfsSink(p, format);
		w.open();
		return w;
	}

	/**
	 * Writes the message to an HDFS file whose path is substituted with tags
	 * drawn from the supplied event
	 */
	@Override
	public void append(Event e) throws IOException, InterruptedException {
		CustomDfsSink w = writer;
		String end = new String(e.get("end"));
		if (shouldSub) {
			String realPath = e.escapeString(absolutePath);
			w = sfWriters.get(realPath);
			if (w == null) {
				w = openWriter(realPath);
				sfWriters.put(realPath, w);
			}
			if (end != null && end.equals(END)) {
				sfWriters.remove(realPath);
			}
		}
		w.append(e);
		super.append(e);
		if (end != null && end.equals(END)) {
			w.close();
		}

	}

	@Override
	public void close() throws IOException {
		if (shouldSub) {
			for (Entry<String, CustomDfsSink> e : sfWriters.entrySet()) {
				LOG.info("Closing " + e.getKey());
				e.getValue().close();
			}
		} else {
			LOG.info("Closing " + absolutePath);
			if (writer == null) {
				LOG.warn("BlockDirSink Writer for '" + absolutePath
						+ "' was already closed!");
				return;
			}

			writer.close();
			writer = null;
		}
	}

	@Override
	public void open() throws IOException {
		if (!shouldSub) {
			writer = openWriter(absolutePath);
		}
	}

	public static SinkBuilder builder() {
		return new SinkBuilder() {
			@Override
			public EventSink create(Context context, Object... args) {
				Preconditions.checkArgument(args.length >= 1
						&& args.length <= 3,
						"usage: BlockDirSink(\"[(hdfs|file|s3n|...)://namenode[:port]]/path\""
								+ "[, file [,outputformat ]])");

				String filename = "";
				if (args.length >= 2) {
					filename = args[1].toString();
				}

				Object format = FlumeConfiguration.get()
						.getDefaultOutputFormat();
				if (args.length >= 3) {
					format = args[2];
				}

				OutputFormat o;
				try {
					o = FlumeBuilder.createFormat(FormatFactory.get(), format);
				} catch (FlumeSpecException e) {
					LOG.warn("Illegal format type " + format + ".", e);
					o = null;
				}
				Preconditions.checkArgument(o != null, "Illegal format type "
						+ format + ".");

				return new BlockDfsSink(args[0].toString(), filename, o);
			}

			@Deprecated
			@Override
			public EventSink build(Context context, String... args) {
				// updated interface calls build(Context,Object...) instead
				throw new RuntimeException(
						"Old sink builder for BlockDirSink should not be exercised");
			}

		};
	}
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
		List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("BlockDfsSink",
				builder()));
		return builders;
	}
	
}
