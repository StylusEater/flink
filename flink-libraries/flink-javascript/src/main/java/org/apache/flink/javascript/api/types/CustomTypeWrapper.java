package org.apache.flink.javascript.api.types;

/**
 * Container for serialized JavaScript objects, generally assumed to be custom objects.
 */
public class CustomTypeWrapper {
	private final byte typeID;
	private final byte[] data;

	public CustomTypeWrapper(byte typeID, byte[] data) {
		this.typeID = typeID;
		this.data = data;
	}

	public byte getType() {
		return typeID;
	}

	public byte[] getData() {
		return data;
	}
}

