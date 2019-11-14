package common;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FieldEntryHolder {
    private final List<Object> originFields;
    private final Iterator<Object> iterator;
    private final List<Object> filteredFields;
    public FieldEntryHolder(List<Object> originFields) {
        this.originFields = originFields;
        if (null == originFields) {
            this.filteredFields = null;
            this.iterator = null;
        } else {
            this.filteredFields = new LinkedList<>();
            this.iterator = originFields.iterator();
        }
    }

    public boolean hasNext() {
        if (iterator == null) {
            return true;
        }
        return iterator.hasNext();
    }

    public void skip() {
        if (null != iterator) {
            iterator.next();
        }
    }

    public Object take() {
        if (null != iterator) {
            Object current = iterator.next();
            filteredFields.add(current);
            return  current;
        } else {
            return null;
        }
    }
}