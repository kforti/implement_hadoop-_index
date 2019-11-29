package index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class IndexCompositeKey implements WritableComparable<IndexCompositeKey> {

    public int attribute;
    public String splitId;

    public IndexCompositeKey() {
    }

    public IndexCompositeKey(String id, int attribute) {
        super();
        this.set(id, attribute);
    }

    public void set(String id, int attribute) {
        this.attribute = attribute;
        this.splitId = id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(splitId);
        out.writeUTF(Integer.toString(attribute));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        splitId = in.readUTF();
        attribute = Integer.parseInt(in.readUTF());

    }

    @Override
    public int compareTo(IndexCompositeKey ckey) {
        return Integer.compare(attribute, ckey.attribute);
    }

}