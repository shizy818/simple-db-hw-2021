package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId tid;
    private HeapFile heapFile;
    private int pageNum;
    private Iterator<Tuple> tupleIterator = null;

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        this.tid = tid;
        this.heapFile = file;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        HeapPage heapPage = getPageByNumber(0);
        if (heapPage != null) {
            tupleIterator = heapPage.iterator();
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tupleIterator == null)
            return false;

        if (tupleIterator.hasNext())
            return true;

        pageNum++;
        if (pageNum >= heapFile.numPages()) {
            return false;
        } else {
            tupleIterator = getPageByNumber(pageNum).iterator();
        }

        return tupleIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return tupleIterator.next();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        HeapPage heapPage = getPageByNumber(0);
        if (heapPage != null) {
            tupleIterator = heapPage.iterator();
        }
    }

    @Override
    public void close() {
        tupleIterator = null;
        pageNum = 0;
    }

    private HeapPage getPageByNumber(int pageNum) throws DbException, TransactionAbortedException{
        if (pageNum < 0 || pageNum > heapFile.numPages()) {
            throw new DbException("Page Number is invalid : pageNumber is " + pageNum + "  numPages is " + heapFile.numPages());
        }
        HeapPageId pageId = new HeapPageId(heapFile.getId(), pageNum);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        return heapPage;
    }
}
