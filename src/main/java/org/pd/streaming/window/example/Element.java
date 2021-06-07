package org.pd.streaming.window.example;

import lombok.Getter;
import lombok.Setter;

@Setter @Getter
public class Element{
        Integer value;
        Long timestamp;
    public Element( int counter, long currTime ){
        this.value = counter;
        this.timestamp = currTime;
        }
    @Override
    public String toString(){
        return "" + value;
    }
}