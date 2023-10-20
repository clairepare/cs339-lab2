package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private File backingFile;
    private TupleDesc tupleDesc;
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        backingFile = f;
        tupleDesc = td;
        tableId = f.getAbsoluteFile().hashCode();

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return backingFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile raf = new RandomAccessFile(backingFile, "r");
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            byte[] d = new byte[BufferPool.getPageSize()];
            if (raf.read(d, offset, BufferPool.getPageSize()) < BufferPool.getPageSize()) {
                raf.close();
                throw new IOException("no such page");
            }
            HeapPageId hpi = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            HeapPage ret = new HeapPage(hpi, d);
            raf.close();
            return ret;
        }
        catch (IOException e) {
            throw new IllegalArgumentException("couldn't read page");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)backingFile.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator();
    }

    private class HeapFileIterator implements DbFileIterator {
        private int curPageNo;
        private HeapPageId currentPid = null;
        private BufferPool bp = Database.getBufferPool();
        private TransactionId tid;
        private HeapPage curPage;
        private Iterator<Tuple> tupleIter = null;

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            try {
                curPageNo = 0;
                currentPid = new HeapPageId(tableId, curPageNo);
                tid = new TransactionId();
                Page p = bp.getPage(tid, currentPid, Permissions.READ_WRITE);
                curPage = new HeapPage(currentPid, p.getPageData());
                tupleIter = curPage.iterator();
            }
            catch (Exception e) {
                throw new DbException("couldn't access DB " + e.getMessage());
            }
        }

        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            //unopened
            if (tupleIter == null) {
                return false;
            }
            
            //more tuples on this page?
            //if so, we're good
            if (tupleIter.hasNext()) {
                return true;
            }
            //if not, we have to see if there's more pages in the file
            else {
                try {
                    curPageNo++;
                    if (curPageNo < numPages()) {
                        //if all this succeeds, we have a new page and are also good
                        currentPid = new HeapPageId(tableId, curPageNo);
                        tid = new TransactionId();
                        Page p = bp.getPage(tid, currentPid, Permissions.READ_WRITE);
                        curPage = new HeapPage(currentPid, p.getPageData());
                        tupleIter = curPage.iterator();
                        return true;
                    }
                    else {
                        return false;
                    }
                }
                catch (Exception e) {
                    throw new DbException("Page fetching issue");
                }
            }
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return tupleIter.next();
            }
            else {
                throw new NoSuchElementException("no more tuples");
            }
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //just basically running open 
            try {
                curPageNo = 0;
                currentPid = new HeapPageId(getId(), curPageNo);
                tid = new TransactionId();
                Page p = bp.getPage(tid, currentPid, Permissions.READ_WRITE);
                curPage = new HeapPage(currentPid, p.getPageData());
                tupleIter = curPage.iterator();
            }
            catch (Exception e) {
                throw new DbException("couldn't access DB");
            }
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            //nothing's needed?
        }
    }
}

