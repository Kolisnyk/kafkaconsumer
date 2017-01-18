import java.io.Serializable;

/**
 * Created by oleksii on 15.01.17.
 */

public class Country implements Serializable{

    private String name;
    private int counter;

    Country(){};

    Country(String name, int counter){
        this.name = name;
        this.counter = counter;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }
}
