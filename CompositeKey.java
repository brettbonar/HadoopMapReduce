/**
 * Copyright 2012 Jee Vang 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *  Unless required by applicable law or agreed to in writing, software 
 *  distributed under the License is distributed on an "AS IS" BASIS, 
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *  See the License for the specific language governing permissions and 
 *  limitations under the License. 
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class CompositeKey implements WritableComparable<CompositeKey> {

	private Text symbol;
	private IntWritable count;
	
	public CompositeKey() { }
	
	public CompositeKey(Text symbol, IntWritable count) {
		this.symbol = symbol;
		this.count = count;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(symbol)
				.append(',')
				.append(count)
				.append('}')
				.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		symbol = WritableUtils.readString(in);
		count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, symbol);
		out.writeLong(count);
	}

	@Override
	public int compareTo(CompositeKey o) {
		int result = count.compareTo(o.count);
		// int result = symbol.compareTo(o.symbol);
		// if(0 == result) {
		// 	result = count.compareTo(o.count);
		// }
		return result;
	}

	/**
	 * Gets the symbol.
	 * @return Symbol.
	 */
	public Text getSymbol() {
		return symbol;
	}

	public void setSymbol(Text symbol) {
		this.symbol = symbol;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

}
