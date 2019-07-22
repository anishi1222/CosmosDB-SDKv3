package cosmos;

public class People {
    String id;
    String name;
    String description;
    String country;

    public People(String id, String name, String description, String country) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.country = country;
    }

    public People() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}
