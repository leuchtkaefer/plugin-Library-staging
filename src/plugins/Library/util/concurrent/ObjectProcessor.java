/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.Library.util.concurrent;

import plugins.Library.util.func.Closure;
import plugins.Library.util.func.Tuples.*;
import static plugins.Library.util.func.Tuples.*;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
** A class that wraps around an {@link Executor}, for processing any given type
** of object, not just {@link Runnable}. Each object must be accompanied by a
** secondary "deposit" object, which is returned with the object when it has
** been processed. Any exceptions thrown are also returned.
**
** @param <T> Type of object to be processed
** @param <E> Type of object to be used as a deposit
** @param <X> Type of exception thrown by {@link #clo}
** @author infinity0
*/
public class ObjectProcessor<T, E, X extends Exception> implements Scheduler {

	final protected BlockingQueue<T> in;
	final protected BlockingQueue<$2<T, X>> out;
	final protected Map<T, E> dep;
	final protected Closure<T, X> clo;
	final protected Executor exec;

	protected volatile boolean open = true;

	// JDK6 replace with ConcurrentSkipListSet
	final private static ConcurrentMap<ObjectProcessor, Boolean> running = new ConcurrentHashMap<ObjectProcessor, Boolean>();
	// This must only be modified in a static synchronized block
	private static Thread auto = null;

	/**
	** Constructs a new processor. The processor itself will be thread-safe
	** as long as the queues and deposit map are not exposed to other threads,
	** and the closure's invoke method is also thread-safe.
	**
	** If the {@code closure} parameter is {@code null}, it is expected that
	** {@link #createJobFor(Object)} will be overridden appropriately.
	**
	** @param input Queue for input items
	** @param output Queue for output/error items
	** @param deposit Map for item deposits
	** @param closure Closure to call on each item
	** @param executor Executor to run each closure call
	** @param autostart Whether to start an {@link #auto()} autohandler
	*/
	public ObjectProcessor(
		BlockingQueue<T> input, BlockingQueue<$2<T, X>> output, Map<T, E> deposit,
		Closure<T, X> closure, Executor executor, boolean autostart
	) {
		in = input;
		out = output;
		dep = deposit;
		clo = closure;
		exec = executor;
		if (autostart) { auto(); }
	}

	/**
	** Safely submits the given item and deposit to the given processer. Only
	** use this when the input queue's {@link BlockingQueue#put(Object)} method
	** does not throw {@link InterruptedException}, such as that of {@link
	** java.util.concurrent.PriorityBlockingQueue}.
	*/
	public static <T, E, X extends Exception> void submitSafe(ObjectProcessor<T, E, X> proc, T item, E deposit) {
		try {
			proc.submit(item, deposit);
		} catch (InterruptedException e) {
			throw new IllegalArgumentException("ObjectProcessor: abuse of submitSafe(). Blame the programmer, who did not know what they were doing", e);
		}
	}

	/**
	** Submits an item for processing, with the given deposit.
	**
	** @throws IllegalStateException if the processor has already been {@link
	**         #close() closed}
	** @throws IllegalArgumentException if the item is already being held
	*/
	public synchronized void submit(T item, E deposit) throws InterruptedException {
		if (!open) { throw new IllegalStateException("ObjectProcessor: not open"); }
		if (dep.containsKey(item)) {
			throw new IllegalArgumentException("ObjectProcessor: object " + item + " already submitted");
		}

		dep.put(item, deposit);
		in.put(item);
	}

	/**
	** Updates the deposit for a given item.
	**
	** @throws IllegalStateException if the processor has already been {@link
	**         #close() closed}
	** @throws IllegalArgumentException if the item is not currently being held
	*/
	public synchronized void update(T item, E deposit) {
		if (!open) { throw new IllegalStateException("ObjectProcessor: not open"); }
		if (!dep.containsKey(item)) {
			throw new IllegalArgumentException("ObjectProcessor: object " + item + " not yet submitted");
		}

		dep.put(item, deposit);
	}

	/**
	** Retrieved a processed item, along with its deposit and any exception
	** that caused processing to abort.
	*/
	public synchronized $3<T, E, X> accept() throws InterruptedException {
		$2<T, X> item = out.take();
		return $3(item._0, dep.remove(item._0), item._1);
	}

	/**
	** Whether there are any unprocessed items (including completed tasks not
	** yet retrieved by the submitter).
	*/
	public synchronized boolean hasPending() {
		return !dep.isEmpty();
	}

	/**
	** Whether there are any completed items that have not yet been retrieved.
	*/
	public synchronized boolean hasCompleted() {
		return !out.isEmpty();
	}

	/**
	** Number of unprocessed tasks.
	*/
	public synchronized int size() {
		return dep.size();
	}

	/**
	** Retrieves an item by calling {@link BlockingQueue#take()} on the input
	** queue. If this succeeds, a job is {@linkplain #createJobFor(Object)
	** created} for it, and sent to {@link #exec} to be executed.
	**
	** This method is provided for completeness, in case anyone needs it;
	** {@link #auto()} should be adequate for most purposes.
	**
	** @throws InterruptedExeception if interrupted whilst waiting
	*/
	public void dispatchTake() throws InterruptedException {
		T item = in.take();
		exec.execute(createJobFor(item));
	}

	/**
	** Retrieves an item by calling {@link BlockingQueue#poll()} on the input
	** queue. If this succeeds, a job is {@linkplain #createJobFor(Object)
	** created} for it, and sent to {@link #exec} to be executed.
	**
	** This method is provided for completeness, in case anyone needs it;
	** {@link #auto()} should be adequate for most purposes.
	**
	** @return Whether a task was retrieved and executed
	*/
	public boolean dispatchPoll() {
		T item = in.poll();
		if (item == null) { return false; }
		exec.execute(createJobFor(item));
		return true;
	}

	/**
	** Creates a {@link Runnable} to process the item and push it onto the
	** output queue, along with any exception that aborted the process.
	**
	** The default implementation invokes {@link #clo} on the item, and then
	** adds the appropriate data onto the output queue.
	*/
	protected Runnable createJobFor(final T item) {
		if (clo == null) {
			throw new IllegalStateException("ObjectProcessor: no closure given, but createJobFor() was not overidden");
		}
		return new Runnable() {
			/*@Override**/ public void run() {
				X ex = null;
				try { clo.invoke(item); }
				// FIXME NORM this could throw RuntimeException
				catch (Exception e) { ex = (X)e; }
				try { out.put($2(item, ex)); }
				catch (InterruptedException e) { throw new UnsupportedOperationException(e); }
			}
		};
	}

	/**
	** Start a new thread to run the {@link #active} processors, if one is not
	** already running.
	*/
	private static synchronized void ensureAutoHandler() {
		if (auto != null) { return; }
		auto = new Thread() {
			@Override public void run() {
				final int timeout = 4;
				int t = timeout;
				while (!running.isEmpty() && (t=timeout) == timeout || t-- > 0) {
					for (Iterator<ObjectProcessor> it = running.keySet().iterator(); it.hasNext();) {
						ObjectProcessor proc = it.next();
						boolean o = proc.open;
						while (proc.dispatchPoll());
						if (!o) { it.remove(); }
					}
					try {
						// sleep 2^10ms for every 2^10 processors
						Thread.sleep(((running.size()-1)>>10)+1<<10);
					} catch (InterruptedException e) {
						// TODO LOW log this somewhere
					}

					if (t > 0) { continue; }
					synchronized (ObjectProcessor.class) {
						// if auto() was called just before we entered this synchronized block,
						// then its ensureAutoHandler() would have done nothing. so we want to keep
						// this thread running to take care of the new addition.
						if (!running.isEmpty()) { continue; }
						// otherwise we can safely discard this thread, since ensureAutoHandler()
						// cannot be called as long as we are in this block.
						auto = null;
						return;
					}
				}
				throw new AssertionError("ObjectProcessor: bad exit in autohandler. this is a bug; please report.");
			}
		};
		auto.start();
	}

	/**
	** Add this processor to the collection of {@link #active} processes, and
	** makes sure there is a thread to handle them.
	**
	** @return Whether the processor was not already being handled.
	*/
	public boolean auto() {
		Boolean r = ObjectProcessor.running.put(this, Boolean.TRUE);
		ObjectProcessor.ensureAutoHandler();
		return r == null;
	}

	/**
	** Stop accepting new submissions or deposit updates. Held items can still
	** be processed and retrieved, and if an {@linkplain #auto() auto-handler}
	** is running, it will run until all such items have been processed.
	*/
	/*@Override**/ public void close() {
		open = false;
	}

	// public class Object

	/**
	** {@inheritDoc}
	**
	** This implementation just calls {@link #close()}.
	*/
	@Override public void finalize() {
		close();
	}

}