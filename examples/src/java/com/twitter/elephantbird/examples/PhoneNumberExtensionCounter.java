package com.twitter.elephantbird.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person.PhoneNumber;
import com.twitter.elephantbird.examples.proto.AddressBookProtos.PersonExt;
import com.twitter.elephantbird.examples.proto.mapreduce.input.LzoPersonProtobufBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For a file of LZO-compressed protobufs of type Person 
 * (see address_book.proto), generate a count of phone numbers and address
 * for each person.  
 *
 * To write the data file, use a ProtobufBlockWriter<Person> as specified in 
 * the documentation for ProtobufBlockWriter.java.  It will write blocks of 100 
 * Person protobufs at a time to a file which will be lzo compressed.
 */
public class PhoneNumberExtensionCounter extends Configured implements Tool {
  @SuppressWarnings("unused")
  private static final Logger LOG = 
	  LoggerFactory.getLogger(PhoneNumberExtensionCounter.class);

  private PhoneNumberExtensionCounter() {}
  
  private final static class PhoneNumberAddressWritable implements Writable {
	private Text phoneNumber;
	private Text address;

	public void setPhoneNumber(Text phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public Text getAddress() {
		return this.address;
	}

	public void setAddress(Text address) {
		this.address = address;
	}

	public PhoneNumberAddressWritable() {
		this.phoneNumber = new Text();
		this.address = new Text();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.phoneNumber.readFields(in);
		this.address.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.phoneNumber.write(out);
		this.address.write(out);
	}
	  
  }
  
  private final static class CountAddressWritable implements Writable {
	private LongWritable count;
	private Text address;

	public void setCount(LongWritable count) {
		this.count = count;
	}

	public void setAddress(Text address) {
		this.address = address;
	}
	
	public CountAddressWritable() {
		this.count = new LongWritable(0);
		this.address = new Text();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.count.readFields(in);
		this.address.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.count.write(out);
		this.address.write(out);
	}
	  
  }

  public static class PhoneNumberCounterMapper extends Mapper<LongWritable, 
  		ProtobufWritable<Person>, Text, PhoneNumberAddressWritable> {
    private final Text name_ = new Text();
    private final Text phoneNumber_ = new Text();
    private final Text address_ = new Text();
    private PhoneNumberAddressWritable pnaw_ = new PhoneNumberAddressWritable();

    @Override
    protected void map(LongWritable key, ProtobufWritable<Person> value, 
    		Context context) throws IOException, InterruptedException {
      Person p = value.get();
      PersonExt ext = p.getExtension(PersonExt.extInfo);
      address_.set(ext.getAddress());
      name_.set(p.getName());
      pnaw_.setAddress(address_);
      
      for (PhoneNumber phoneNumber: p.getPhoneList()) {
        // Could do something with the PhoneType here; 
    	// note that the protobuf data structure is fully preserved
        // inside the writable, including nesting.
        // PhoneType type = phoneNumber.getType()
        phoneNumber_.set(phoneNumber.getNumber());
        pnaw_.setPhoneNumber(phoneNumber_);
        context.write(name_, pnaw_);
      }
    }
  }

  public static class PhoneNumberCounterReducer extends Reducer<Text, 
  		PhoneNumberAddressWritable, Text, CountAddressWritable> {
    private final LongWritable count_ = new LongWritable();
    private final CountAddressWritable caw_ = new CountAddressWritable();

    @Override
    protected void reduce(Text key, Iterable<PhoneNumberAddressWritable> values,
    		Context context) throws IOException, InterruptedException {
      int count = 0;
      Text address = null; 
      for (PhoneNumberAddressWritable pnaw: values) {
        count++;
        address = pnaw.getAddress();
      }
      
      count_.set(count);
      caw_.setAddress(address);
      caw_.setCount(count_);
      context.write(key, caw_);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: hadoop jar path/to/this.jar " + 
		  getClass() + " <input dir> <output dir>");
      System.exit(1);
    }

    Configuration conf = getConf();
    conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec," + 
            "com.hadoop.compression.lzo.LzopCodec");
    Job job = new Job(conf);
    job.setJobName("Protobuf Person Phone Number Counter");
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PhoneNumberAddressWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CountAddressWritable.class);

    job.setJarByClass(getClass());
    job.setMapperClass(PhoneNumberCounterMapper.class);
    job.setReducerClass(PhoneNumberCounterReducer.class);

    job.setInputFormatClass(LzoPersonProtobufBlockInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PhoneNumberExtensionCounter(), args);
    System.exit(exitCode);
  }
}
