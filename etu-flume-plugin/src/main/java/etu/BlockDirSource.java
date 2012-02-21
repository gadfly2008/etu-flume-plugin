package etu;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;
import com.cloudera.util.dirwatcher.DirChangeHandler;
import com.cloudera.util.dirwatcher.DirWatcher;
import com.cloudera.util.dirwatcher.FileFilter;
import com.cloudera.util.dirwatcher.RegexFileFilter;
import com.google.common.base.Preconditions;

public class BlockDirSource extends EventSource.Base {
	public static final Logger LOG = LoggerFactory
			.getLogger(BlockDirSource.class);
	private DirWatcher watcher;
	private ConcurrentMap<String, DirWatcher> subdirWatcherMap;
	private volatile boolean done = false;
	private FileFilter filt = new RegexFileFilter(".*");
	final SynchronousQueue<File> fileSync = new SynchronousQueue<File>();
	final SynchronousQueue<Event> enentSync = new SynchronousQueue<Event>();
	private final int recurseDepth = 20;
	private final String END	=	"YES";
	private final String NOT_END	=	"NO";

	static int bufferSize = 1024 * 1024 * 4;
	private String dir;
	private String relativePath;

	private int bufSize;

	private BlockThread bhd = null;

	public BlockDirSource(String dir, int size) {
		this.dir = dir;
		this.bufSize = size;
	}

	class BlockThread extends Thread {
		BlockThread() {
			LOG.info("BlockThread create ! ");
		}

		@Override
		public void run() {
			while (!done) {
				try {
					File file = fileSync.poll(100, TimeUnit.MILLISECONDS);
					if (file == null) {
						continue;
					}
					FileInputStream ins = new FileInputStream(file);
					String filePath = file.getAbsolutePath();
					for (;;) {
						try {
							byte[] s = new byte[bufSize];
							int readed = ins.read(s);
							if (readed == -1) {
								ins.close();
								ins = null;
								break;
							}
							byte[] ss;
							if (bufSize == readed) {
								ss = s;
							} else {
								ss = new byte[readed];
								for (int ii = 0; ii < readed; ii++) {
									ss[ii] = s[ii];
								}
							}
							Event e = new EventImpl(ss);
							
							e.set("filePath", filePath.getBytes());
							
							if(readed<bufSize){
								e.set("end", END.getBytes());
							}else{
								e.set("end", NOT_END.getBytes());
							}
							enentSync.put(e);
						} catch (IOException e) {
							LOG.warn(e.getMessage(), e);
						}
					}
				} catch (FileNotFoundException e) {
					LOG.warn("FileNotFoundException :" + e.getMessage(), e);
				} catch (InterruptedException e) {
					LOG.warn("InterruptedException :" + e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * Must be synchronized to isolate watcher
	 */
	@Override
	synchronized public void open() throws IOException {
		Preconditions.checkState(watcher == null,
				"Attempting to open an already open BlockDirSource (" + dir
						+ ", \"" + filt + "\")");
		subdirWatcherMap = new ConcurrentHashMap<String, DirWatcher>();
		watcher = createWatcher(new File(dir), filt, recurseDepth);
		watcher.start();
		bhd = new BlockThread();
		bhd.start();
	}

	private DirWatcher createWatcher(File dir, final FileFilter filt,
			final int recurseDepth) {
		// 5000 ms between checks
		DirWatcher watcher = new DirWatcher(dir, filt, 5000);
		watcher.addHandler(new DirChangeHandler() {
			@Override
			public void fileCreated(File f) {
				if (f.isDirectory()) {
					if (recurseDepth <= 0) {
						LOG.debug("Not watch into subdirectory " + f
								+ ", this watcher recurseDepth: "
								+ recurseDepth);
						return;
					}

					LOG.info("added dir " + f + ", recurseDepth: "
							+ (recurseDepth - 1));
					DirWatcher watcher = createWatcher(f, filt,
							recurseDepth - 1);
					watcher.start();
					subdirWatcherMap.put(f.getPath(), watcher);
					return;
				}

				LOG.info("added file " + f);
				try {
					fileSync.put(f);
				} catch (InterruptedException e) {
					LOG.error("sync put error : " + e.getMessage());
				}
			}

			@Override
			public void fileDeleted(File f) {
				String fileName = f.getPath();
				DirWatcher watcher = subdirWatcherMap.remove(fileName);
				if (watcher != null) {
					LOG.info("removed dir " + f);
					LOG.info("stopping watcher for dir: " + f);
					watcher.stop();
					// calling check explicitly to notify about deleted subdirs,
					// so that subdirs watchers can be stopped
					watcher.check();
					return;
				}
				LOG.info(f.getAbsolutePath() + " deleted !");
			}

		});

		return watcher;
	}

	/**
	 * Must be synchronized to isolate watcher
	 * 
	 * @throws InterruptedException
	 */
	@Override
	synchronized public void close() throws IOException, InterruptedException {
		synchronized (this) {
			done = true;
			if (bhd == null) {
				LOG.warn("BlockDirSource double closed");
				return;
			}
			while (bhd.isAlive()) {
				bhd.join(100L);
				bhd.interrupt();
			}
			bhd = null;
		}
		this.watcher.stop();
		this.watcher = null;
		for (DirWatcher watcher : subdirWatcherMap.values()) {
			watcher.stop();
		}
		subdirWatcherMap = null;
	}

	@Override
	public Event next() throws InterruptedException {

		try {
			while (!done) {
				Event e = enentSync.poll(100, TimeUnit.MILLISECONDS);
				if (e == null)
					continue; // nothing there, retry.
				String filePath	=	new String(e.get("filePath"));
				
				String relativeFileName	=	filePath.substring(dir.length()-1);
				relativePath = relativeFileName.substring(0,relativeFileName.lastIndexOf("/"));
				String filename	=	relativeFileName.substring(relativeFileName.lastIndexOf("/")+1);
				e.set("relativePath", relativePath.getBytes());
				e.set("filename", filename.getBytes());
				
				if(e.getBody().length<this.bufSize){
					(new File(filePath)).delete();
				}
				updateEventProcessingStats(e);
				return e;
			}
			return null; // closed
		} catch (InterruptedException e1) {
			LOG.warn("next unexpectedly interrupted :" + e1.getMessage(), e1);
			throw e1;
		}
	}


	public static SourceBuilder builder() {
		return new SourceBuilder() {
			@Override
			public EventSource build(Context ctx, String... argv) {
				Preconditions
						.checkArgument(argv.length >= 1 && argv.length <= 2,
								"BlockDirSrouce(dir, bufferSize) ");
				int bufSize = bufferSize;
				String dir	=	argv[0];
				if(!dir.endsWith("/")){
					dir	=	dir+"/";
				}
				if (argv.length == 2) {
					bufSize = Integer.valueOf(argv[1]);
					if (bufSize <= 0) {
						bufSize = bufferSize;
					}
				}
				return new BlockDirSource(dir, bufSize);
			}
		};
	}

	public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
		List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
		builders.add(new Pair<String, SourceBuilder>("BlockDirSource",
				builder()));
		return builders;
	}
}