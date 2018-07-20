package org.xmr.study.kafka_demo.message;

import java.io.Serializable;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class HiMessage implements Serializable {
	
	private static final long serialVersionUID = -8940196742312994734L;
	
    public String getMsgId() {
		return msgId;
	}
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	public HiMessageType getType() {
		return type;
	}
	public void setType(HiMessageType type) {
		this.type = type;
	}
	public String getRequestId() {
		return requestId;
	}
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public byte[] getBody() {
		return body;
	}
	public void setBody(byte[] body) {
		this.body = body;
	}
    public HiMessageStatus getStatus() {
		return status;
	}
	public void setStatus(HiMessageStatus status) {
		this.status = status;
	}

    public String toString() {
        ReflectionToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        return ReflectionToStringBuilder.toStringExclude(this, new String[]{"body"});
    }
	
	private String msgId;
    private HiMessageType type;
    private String requestId;
    private long timeStamp;
	private HiMessageStatus status;
    private byte[] body;
}
