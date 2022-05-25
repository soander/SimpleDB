package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    // A help class to facilitate organizing the information of each field
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;
        public final Type fieldType; // The type of the field
        public final String fieldName; // The name of the field

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final ArrayList<TDItem> tdItemArrayList;

    /** @return An iterator which iterates over all the
    field TDItems that are included in this TupleDesc */
    public Iterator<TDItem> iterator() {
        return tdItemArrayList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
    * @Author Yaozheng Wang
    * @Description Create a new TupleDesc with typeAr.length fields with fields of the
    *              specified types, with associated named fields.
    * @Date 2022/5/24 19:58
    * @Param typeAr
    *       array specifying the number of and types of fields in this
    *       TupleDesc. It must contain at least one entry.
    * @Param fieldAr
    *       array specifying the names of the fields. Note that names may be null.
    **/
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr == null || fieldAr == null) {
            throw new IllegalArgumentException("The type array or field array is null.");
        }
        tdItemArrayList = new ArrayList<>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            tdItemArrayList.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
    * @Author Yaozheng Wang
    * @Description Constructor: Create a new tuple desc with typeAr.length fields with
    *   fields of the specified types, with anonymous (unnamed) fields.
    * @Date 2022/5/24 20:29
    * @Param typeAr: array specifying the number of and types of fields in this
    *   TupleDesc. It must contain at least one entry.
    **/
    public TupleDesc(Type[] typeAr) {
        if (typeAr == null) {
            throw new IllegalArgumentException("The type array is null.");
        }
        tdItemArrayList = new ArrayList<>(typeAr.length);
        for (Type type : typeAr) {
            tdItemArrayList.add(new TDItem(type, "anonymous"));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItemArrayList.size();
    }

    /**
    * @Author Yaozheng Wang
    * @Description Gets the (possibly null) field name of the ith field of this TupleDesc.
    * @Date 2022/5/24 20:43
    * @Param i: index of the field name to return. It must be a valid index.
    * @Return the name of the ith field
    * @Throws NoSuchElementException: if i is not a valid field reference.
    **/
    public String getFieldName(int i) throws NoSuchElementException {
        try {
            return tdItemArrayList.get(i).fieldName;
        } catch (Exception e) {
            throw new NoSuchElementException("Invalid field reference.");
        }
    }

    /**
    * @Author Yaozheng Wang
    * @Description Gets the type of the ith field of this TupleDesc.
    * @Date 2022/5/24 20:47
    * @Param i: The index of the field to get the type of. It must be a valid index.
    * @Throws NoSuchElementException: if i is not a valid field reference.
    **/
    public Type getFieldType(int i) throws NoSuchElementException {
        try {
            return tdItemArrayList.get(i).fieldType;
        } catch (Exception e) {
            throw new NoSuchElementException("Invalid field reference.");
        }
    }

    /**
    * @Author Yaozheng Wang
    * @Description Find the index of the field with a given name.
    * @Date 2022/5/24 20:48
    * @Param name: name of the field.
    * @Return the index of the field that is first to have the given name.
    * @Throws NoSuchElementException: if no field with a matching name is found.
    **/
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tdItemArrayList.size(); i++) {
            if (tdItemArrayList.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No field with a marching name is found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for (TDItem item: tdItemArrayList) {
            totalSize += item.fieldType.getLen();
        }
        return totalSize;
    }

    /**
    * @Author Yaozheng Wang
    * @Description Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
    *   with the first td1.numFields coming from td1 and the remaining from td2.
    * @Date 2022/5/24 21:04
    * @Param td1: The TupleDesc with the first fields of the new TupleDesc
    * @Param td2: The TupleDesc with the last fields of the TupleDesc
    * @Return the new TupleDesc
    **/
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] names = new String[td1.numFields() + td2.numFields()];

        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.numFields(); i++) {
            types[td1.numFields() + i] = td2.getFieldType(i);
            names[td1.numFields() + i] = td2.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    /**
    * @Author Yaozheng Wang
    * @Description Compares the specified object with this TupleDesc for equality. Two
    *     TupleDescs are considered equal if they have the same number of items
    *     and if the i-th type in this TupleDesc is equal to the i-th type in o for every i.
    * @Date 2022/5/24 21:21
    * @Param o: the Object to be compared for equality with this TupleDesc.
    * @Return true if the object is equal to this TupleDesc.
    **/
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc tupleDesc = (TupleDesc) o;

        if (tupleDesc.numFields() == this.numFields()) {
            for (int i = 0; i < tupleDesc.numFields(); i++) {
                if (tupleDesc.getFieldType(i) != this.getFieldType(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
    * @Author Yaozheng Wang
    * @Description Returns a String describing this descriptor. It should be of the form
     *      "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     *      the exact format does not matter.
    * @Date 2022/5/24 21:25
    * @Param null
    * @Return String describing this descriptor.
    **/
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (TDItem item : tdItemArrayList) {
            buffer.append(item.toString());
            buffer.append(", ");
        }
        return buffer.toString();
    }
}
