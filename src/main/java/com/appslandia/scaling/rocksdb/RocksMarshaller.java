// The MIT License (MIT)
// Copyright © 2015 AppsLandia. All rights reserved.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package com.appslandia.scaling.rocksdb;

/**
 *
 * @author <a href="mailto:haducloc13@gmail.com">Loc Ha</a>
 *
 */
public interface RocksMarshaller<T> {

	byte[] marshal(T value);

	T unmarshal(byte[] value);

	public static final RocksMarshaller<String> TYPE_STRING = new RocksMarshaller<String>() {

		@Override
		public byte[] marshal(String value) {
			return RocksUtils.marshal(value);
		}

		@Override
		public String unmarshal(byte[] value) {
			return RocksUtils.unmarshal(value);
		}
	};

	public static final RocksMarshaller<byte[]> TYPE_ITSELF = new RocksMarshaller<byte[]>() {

		@Override
		public byte[] marshal(byte[] value) {
			return value;
		}

		@Override
		public byte[] unmarshal(byte[] value) {
			return value;
		}
	};
}
