package model;

/**
 * Created by tsotzo on 8/5/2017.
 */
public class DataType {
    private String subject;
    private String table;
    private String object;

    public DataType() {
    }

    public DataType(String subject, String table, String object) {
        this.subject = subject;
        this.table = table;
        this.object = object;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }
}
