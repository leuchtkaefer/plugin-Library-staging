/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.Library.serial;

/**
** Basic progress implementation. The number of parts known is implicily equal
** to the number of parts started.
**
** '''USE OF THIS CLASS IS PRONE TO DEADLOCK - MAKE SURE YOU MATCH UP YOUR
** CALLS TO {@link #addPartDone()} AND {@link #addPartKnown(int, boolean)}.'''
** I will deal with this at some point, and make this class easier to use...
** possibly have a setDone() method like AtomicProgress has...
**
** NOTE: this implementation considers the task to be '''done''' when a call to
** {@link #addPartDone()} triggers the condition {@code pdone == known} when
** {@code known == estimate} is true. '''As soon as''' this is triggered, all
** threads blocked on {@link #join()} will unblock and assume the task has
** finished. To prevent this from happening unexpectedly, it is recommended
** that {@link #enteredSerialiser()} and {@link #exitingSerialiser()} be called
** '''for each serialiser visited'''. Failure to follow this advice may result
** in deadlock or task data being retrieved before the task is properly
** complete. For more details, see the source code.
**
** TODO perhaps synchronize
**
** @author infinity0
*/
public class SimpleProgress implements Progress {

	/*
	** More details about the note in the class description:
	**
	** If you pass the progress object onto a child serialiser, but do extra
	** processing of the task data after the child returns, it is **vital**
	** that you call enteredSerialiser() and exitingSerialiser(). Otherwise,
	** if any child serialiser triggers the aforementioned condition, it will
	** cause all threads blocked on join() to unblock and think the task has
	** been completed, before the extra processing has occured. ie:
	**
	** * Parent: [progress is at 8/8, not final]
	** * Parent: subsrl.pullLive(task, progress)
	** * Child: addPartKnown(8, true); [progress is at 8/16, final]
	** * Child: addPartDone() * 8; [progress is at 16/16, final]
	** * **all threads blocked on join() will unblock**
	** * Parent: yay, child has done its stuff, now I need to do some extra
	**   stuff to actually complete the task
	** * other threads: "wtf? data's changing randomly!?"
	**
	** (We don't wait for the child serialiser to finish to finalise the total,
	** because we want the total to be finalised as soon as possible. Generally, we
	** let the leaf serialiser finalise the total.)
	*/

	protected String subject = "??";

	/**
	** Number of parts done.
	*/
	protected volatile int pdone;

	/**
	** Number of parts known. For {@link SimpleProgress}, this is implicitly
	** also the number of parts started.
	*/
	protected volatile int known;

	/**
	** Estimated total number of parts. By defalt, this is {@code -1}, meaning
	** no estimate.
	*/
	protected volatile int estimate = -1;

	protected String status = null;

	protected boolean inprogress = true; // this variable is only used in synchronized blocks so doesn't need to be volatile

	protected volatile TaskAbortException abort = null;

	public SimpleProgress() { }

	/**
	** Add a done part. If {@code pdone == known} is triggered while {@code
	** known == estimate} is true, then the task is deemed to be completed and
	** all threads blocked on {@link #join()} will be released.
	*/
	public synchronized void addPartDone() {
		if (pdone == known) {
			throw new IllegalStateException("Can't increased parts done above parts known");
		}
		pdone++;
		if (known == estimate && pdone == known) {
			inprogress = false;
			notifyAll();
		}
	}

	/**
	** Add some known parts, and either finalise the total (set estimate to be
	** equal to it), or invalidate the estimate (set it to -1).
	*/
	public synchronized void addPartKnown(int parts, boolean finalise) {
		if (parts < 0) {
			throw new IllegalArgumentException("Can't decrease the number of parts done.");
		} else if (finalise) {
			if (known == estimate && parts > 0) {
				throw new IllegalArgumentException("Total already finalised");
			}
			// update estimate first to maintain known <= estimate || estimate == -1
			estimate = known + parts;
			known = estimate;
		} else {
			if (known == estimate) {
				throw new IllegalArgumentException("Cannot un-finalise a final total!");
			}
			estimate = -1;
			known += parts;
		}
	}

	/**
	** Set an estimate. This should be used '''very''' carefully due to the
	** automatic thread-unblocking behaviour of {@link #addPartDone()}.
	*/
	public void setEstimate(int e) {
		// no synchronization; we don't care about missed updates
		if (e < known) {
			throw new IllegalArgumentException("Can't give a lower estimate than what is already known.");
		}
		estimate = e;
	}

	/**
	** This method should be called first thing after entering a serialiser.
	** Otherwise, deadlock may result, or thread-safety may be broken.
	*/
	public void enteredSerialiser() {
		addPartKnown(1, false);
	}

	/**
	** This method should be called last thing before leaving a serialiser.
	** Otherwise, deadlock may result, or thread-safety may be broken.
	**
	** (Note: normal exits; not exceptions. Ie. call this in a {@code try}
	** block, not a {@code finally} block.)
	*/
	public void exitingSerialiser() {
		addPartDone();
	}

	public synchronized void abort(TaskAbortException e) throws TaskAbortException {
		abort = e;
		inprogress = false;
		notifyAll();
		throw e;
	}

	public void setSubject(String s) {
		if (s == null) {
			throw new IllegalArgumentException("Can't set a null progress subject");
		}
		subject = s;
	}

	public void setStatus(String s) {
		status = s;
	}

	public void setStatusDefault() {
		status = null;
	}

	/*========================================================================
	  public interface Progress
	 ========================================================================*/

	@Override public String getSubject() {
		return subject;
	}

	/**
	** {@inheritDoc}
	**
	** This implementation (in order of priority), the message for {@link
	** #error}, {@link #status}, or string constructed from {@link #pdone},
	** {@link #known} and {@link #estimate}.
	*/
	@Override public String getStatus() {
		if (abort != null) { return abort.getMessage(); }
		if (status != null) { return status; }
		try {
			return getParts().toString();
		} catch (TaskAbortException e) {
			// abort could have been set between lines 1 and 4
			return abort.getMessage();
		}
	}

	@Override public ProgressParts getParts() throws TaskAbortException {
		// updates are made such that the ProgressParts contract isn't broken even
		// in mid-update so we don't need to synchronize here.
		if (abort != null) { throw abort; }
		return new ProgressParts(pdone, known, known, estimate);
	}

	@Override public boolean isDone() throws TaskAbortException {
		// updates are made such that the ProgressParts contract isn't broken even
		// in mid-update so we don't need to synchronize here.
		if (abort != null) { throw abort; }
		return pdone == known && known == estimate;
	}

	@Override public synchronized void join() throws InterruptedException, TaskAbortException {
		while (inprogress) { wait(); }
		if (abort != null) { throw abort; }
	}

}
