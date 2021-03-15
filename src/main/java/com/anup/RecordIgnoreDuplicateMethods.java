package com.anup;

import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

interface RecordIgnoreDuplicateMethods {
    @JsonIgnore // this overloaded method causes a problem deserializing json
    public void setEventName(OperationType eventName);
    @JsonProperty("eventName")
    public void setEventName(String eventName);
}