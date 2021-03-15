package com.anup;

import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

interface StreamRecordIgnoreDuplicateMethods {
    @JsonIgnore // this overloaded method causes a problem deserializing json
    public void setStreamViewType(StreamViewType eventName);
    @JsonProperty("streamViewType")
    public void setStreamViewType(String eventName);
}