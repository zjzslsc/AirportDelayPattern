package project.source;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ProcessingIntermediumKey implements WritableComparable<ProcessingIntermediumKey> {
	public int year;
	public int month;
	public int day;
	public int time;
	public String airport;
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		time = in.readInt();
		airport = in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(time);
		out.writeUTF(airport);
	}
	@Override
	public int compareTo(ProcessingIntermediumKey p) {
		// TODO Auto-generated method stub
		if (year != p.year)
			return year > p.year ? 1 : -1;
		if (month != p.month)
			return month > p.month ? 1 : -1;
		if (day != p.day)
			return day > p.day ? -1 : 1;
		int cmp = airport.compareTo(p.airport);
		if (cmp != 0)
			return cmp;
		if (time != p.time)
			return time > p.time ? -1 : 1;
		return 0;
	}
}
