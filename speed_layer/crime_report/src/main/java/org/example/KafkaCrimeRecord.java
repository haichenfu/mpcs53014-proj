package org.example;

public class KafkaCrimeRecord {
    private String id;
    private String date;
    private String primaryType;
    private String locationDescription;

    public KafkaCrimeRecord(String id, String date, String primaryType, String locationDescription) {
        this.id = id;
        this.date = date;
        this.primaryType = primaryType;
        this.locationDescription = locationDescription;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPrimaryType() {
        return primaryType;
    }

    public void setPrimaryType(String primaryType) {
        this.primaryType = primaryType;
    }

    public String getLocationDescription() {
        return locationDescription;
    }

    public void setLocationDescription(String locationDescription) {
        this.locationDescription = locationDescription;
    }
}
