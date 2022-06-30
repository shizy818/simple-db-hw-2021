package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId tid;
    private HeapFile heapFile;
    private int pageNo = -1;
    private Iterator<Tuple> tupleIter = null;

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        this.tid = tid;
        this.heapFile = file;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pageNo = 0;
        HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
        tupleIter = page.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!tupleIter.hasNext()) {
            // get next page
            while (pageNo < heapFile.numPages()) {
                pageNo++;
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
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
        HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
        tupleIter = page.iterator();
    }

    @Override
    public void close() {
        pageNo = -1;
        tupleIter = null;
    }
}
