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

public class CompositeKey implements WritableComparable<CompositeKey> {

	private String symbol;
	private Long count;
	
	public CompositeKey() { }
	
	public CompositeKey(String symbol, Long count) {
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
		int result = symbol.compareTo(o.symbol);
		if(0 == result) {
			result = count.compareTo(o.count);
		}
		return result;
	}

	/**
	 * Gets the symbol.
	 * @return Symbol.
	 */
	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

}
