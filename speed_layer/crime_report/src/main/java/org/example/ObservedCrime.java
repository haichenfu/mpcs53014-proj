package org.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.annotation.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id",
        "date",
        "primary_type",
        "location_description"
})
@Generated("jsonschema2pojo")
public class ObservedCrime {
    @JsonProperty("id")
    private String id;
    @JsonProperty("date")
    private String date;
    @JsonProperty("primary_type")
    private String primary_type;
    @JsonProperty("location_description")
    private String location_description;

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }
    @JsonProperty("date")
    public String getDate() {
        return date;
    }
    @JsonProperty("date")
    public void setDate(String date) {
        this.date = date;
    }
    @JsonProperty("primary_type")
    public String getPrimary_type() {
        return primary_type;
    }
    @JsonProperty("primary_type")
    public void setPrimary_type(String primary_type) {
        this.primary_type = primary_type;
    }
    @JsonProperty("location_description")
    public String getLocation_description() {
        return location_description;
    }
    @JsonProperty("location_description")
    public void setLocation_description(String location_description) {
        this.location_description = location_description;
    }



}
