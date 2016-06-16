package com.datorama.oss.redissonreplicatedmap;

import java.io.Serializable;

public class Message implements Serializable {

	private static final long serialVersionUID = -8173125100092443549L;

	public static enum Type {
		PUT,
		REMOVE,
		CLEAR,
		PUT_ALL
	}

	private Type type;
	private Object key;
	private Object value;
	private String source;

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Message() {

	}

	public Message(Type type, Object key, Object value, String source) {
		this.type = type;
		this.key = key;
		this.value = value;
		this.source = source;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

}
