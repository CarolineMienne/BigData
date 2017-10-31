import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class KString implements Comparable<KString>, Writable{
        public Long key;
        public String string;

        public KString()
        {
                key = (long) 0;
                string = "";
        }

	public KString (long key, String string)
        {
                this.key = key;
                this.string = string;
        }

	@Override
         public boolean equals(Object b){
                if(!(b instanceof KString) || this.compareTo((KString)b) != 0){
                         return false;
                }
                return true;
        }

	@Override
        public int compareTo(KString other)
        {
                return this.key.compareTo(other.key);
        }

	@Override
        public void readFields(DataInput in) throws IOException {
                key = in.readLong();
                string = in.readUTF();
        }

	@Override
        public void write(DataOutput out) throws IOException {
                out.writeLong(key);
                out.writeUTF(string);

        }
}
