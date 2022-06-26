package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
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
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        HeapPageId hpid = (HeapPageId) pid;
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            byte[] data = new byte[pageSize];
            // randomAccessFile.read(data, offset, pageSize);
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            randomAccessFile.close();

            page = new HeapPage(hpid, data);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        byte[] data = page.getPageData();
        int offset = BufferPool.getPageSize() * page.getId().getPageNumber();
        try {
            RandomAccessFile disk = new RandomAccessFile(this.file, "rw");
            disk.seek(offset);
            disk.write(data);
            disk.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(
                (double)file.length() / (double)BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> dirtyPages = new ArrayList<>();

        for(int i = 0; i < numPages(); i++) {
            HeapPageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, null);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);

                // add dirty page to list
                dirtyPages.add(page);
                break;
            }
        }

        // if all pages are full, allocate new page
        if (dirtyPages.size() == 0) {
            HeapPageId pageId = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(pageId, HeapPage.createEmptyPageData());
            newPage.insertTuple(t);
            this.writePage(newPage);

            // add dirty page to list
            dirtyPages.add(newPage);
        }

        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyPages = new ArrayList<>();

        // find the page and delete the tuple
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        // add dirty page to list
        dirtyPages.add(page);

        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int pageNo = -1;
            private Iterator<Tuple> tupleIter = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNo = 0;
                HeapPageId pid = new HeapPageId(getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
                tupleIter = page.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!tupleIter.hasNext()) {
                    // get next page
                    while (pageNo < numPages()) {
                        pageNo++;
                        HeapPageId pid = new HeapPageId(getId(), pageNo);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
                        tupleIter = page.iterator();
                        if (tupleIter.hasNext()) {
                            break;
                        }
                    }
                }
                return tupleIter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("Out of tuples boundary");
                }
                return tupleIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageNo = 0;
                HeapPageId pid = new HeapPageId(getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
                tupleIter = page.iterator();
            }

            @Override
            public void close() {
                pageNo = -1;
                tupleIter = null;
            }
        };
    }

}

