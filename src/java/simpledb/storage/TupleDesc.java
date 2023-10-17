package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private TDItem[] typefieldArray;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return new TDItemIterator();
    }

    private class TDItemIterator implements Iterator<TDItem> {
        private int currentIndex = 0;

        @Override
        public boolean hasNext() {
            return currentIndex < typefieldArray.length-1;
        }

        @Override
        public TDItem next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return typefieldArray[currentIndex++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove operation is not supported.");
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        typefieldArray = new TDItem[typeAr.length];

        for(int i = 0; i < typefieldArray.length; i++){
            typefieldArray[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        typefieldArray = new TDItem[typeAr.length];

        for(int i = 0; i < typefieldArray.length; i++){
            typefieldArray[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return typefieldArray.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        try {
            return typefieldArray[i].fieldName;
        }
        catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException("No such field found");
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        try {
            return typefieldArray[i].fieldType;
        }
        catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException("No such type found when trying to get field type from index " + i);
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for(int i = 0; i < typefieldArray.length; i++){
            if(typefieldArray[i].fieldName != null && typefieldArray[i].fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("Index not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int total = 0;
        for(int i = 0; i < typefieldArray.length; i++){
            total += typefieldArray[i].fieldType.getLen();
        }
        return total;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] newTypeArray = new Type[td1.numFields() + td2.numFields()];
        String[] newFieldArray = new String[td1.numFields() + td2.numFields()];
        for(int i = 0; i < td1.numFields(); i++){
            newTypeArray[i] = td1.getFieldType(i);
            newFieldArray[i] = td1.getFieldName(i);
        }
        for(int i = td1.numFields(); i < newTypeArray.length; i++){
            newTypeArray[i] = td2.getFieldType(i-td1.numFields());
            newFieldArray[i] = td2.getFieldName(i-td1.numFields());
        }
        TupleDesc newtd = new TupleDesc(newTypeArray, newFieldArray);
        return newtd;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {

        if (this == o) {
            return true; //same object reference
        }

        if (!(o instanceof TupleDesc)) {
            return false; // can't be equal if o != TupleDesc
        }

        TupleDesc td = (TupleDesc)o;
        if(td.numFields() != typefieldArray.length){
            return false;
        }
        else{
            for(int i = 0; i < typefieldArray.length; i++){
                if(td.getFieldType(i) != typefieldArray[i].fieldType){
                    return false;
                }
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String ret = "";
        for(int i = 0; i < typefieldArray.length; i++){
            ret += typefieldArray[i].fieldType + "(" + typefieldArray[i].fieldName + "), ";
        }
        return ret.substring(0, ret.length()-2);
    }
}
