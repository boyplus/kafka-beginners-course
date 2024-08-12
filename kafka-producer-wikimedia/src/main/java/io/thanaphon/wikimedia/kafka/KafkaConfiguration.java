package io.thanaphon.wikimedia.kafka;

public class KafkaConfiguration {
    public String server;
    public String username;
    public String password;

    public KafkaConfiguration(String server, String username, String password){
        this.server = server;
        this.username = username;
        this.password = password;
    }

    public void print(){
        System.out.printf("Server: %s, username: %s, password: %s%n", this.server, this.username, this.password);
    }
}
