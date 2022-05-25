package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc tupleDesc;
    private static RecordId recordId;
    private final Field[] fields;
    private static final long serialVersionUID = 1L;

    /**
    * @Author Yaozheng Wang
    * @Description Create a new tuple with the specified schema (type).
    * @Date 2022/5/24 21:34
    * @Param td: the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
    **/
    public Tuple(TupleDesc td) {
        if (td == null) {
            throw new IllegalArgumentException("Invalid tuple Description");
        }
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid: the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i: index of the field to change. It must be a valid index.
     * @param f: new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i > fields.length) {
            throw new IllegalArgumentException("Index is invalid.");
        }
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * @param i: field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }



    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            stringBuilder.append(fields[i].toString()).append("\t");
        }
        stringBuilder.append('\n');
        return stringBuilder.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return Arrays.stream(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td) {
        tupleDesc = td;
    }


}
