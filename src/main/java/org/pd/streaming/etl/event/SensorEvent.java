package org.pd.streaming.etl.event;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Simple POJO to model events, useful for JSON to Java transformation
 * 
 * @author preetdeep.kumar
 */
public class SensorEvent implements Serializable
{         
    private static final long serialVersionUID = 9096676048598731010L;

    /* Either connected or disconnected */
    String type;
    
    /* Time of event generated at source */
    String timestamp;
    
    /* Some unique ID of the sensor or device for which this event was generated */
    String deviceId;

    public SensorEvent()
    {
    }
    
    public SensorEvent(String type, String deviceId)
    {
        this.type = type;
        this.timestamp = LocalDateTime.now().toString();
        this.deviceId = deviceId;
    }        

    public String getTimestamp()
    {
        return timestamp;
    }

    public String getDeviceId()
    {
        return deviceId;
    }

    public boolean isConnected()
    {
        return this.type.equals( "CONNECTED" );
    }
    
    public boolean isDisconnected()
    {
        return this.type.equals( "DISCONNECTED" );
    }

    public String getType()
    {
        return type;
    }
    
    @Override
    public String toString()
    {
        return " " + deviceId + ":" + type + " ";
    }
}
