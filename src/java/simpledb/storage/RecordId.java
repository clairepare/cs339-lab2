package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pageId;
    private int tNumber;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        pageId = pid;
        tNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return tNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true; //same object reference
        }

        if (!(o instanceof RecordId)) {
            return false; // can't be equal if o != HeapPageId
        }

        RecordId td = (RecordId) o;
        //same tuple is the same tuplenumber and the same page number

        if(td.getTupleNumber() == this.getTupleNumber() && td.getPageId().equals(this.getPageId())){
            return true;
        }
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return 31 * this.getTupleNumber() + 17 * this.getPageId().getTableId() + this.getPageId().getPageNumber();
    }

}
