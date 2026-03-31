package cloud.domain;

import java.io.Serializable;

public class Operation implements Serializable {
    public String type;
    public byte[] function;
    public String language;

    // Default constructor for Jackson
    public Operation() {
        this.language = Task.LANGUAGE_JAVA;
    }

    public Operation(String type, byte[] function) {
        this(type, function, Task.LANGUAGE_JAVA);
    }

    public Operation(String type, byte[] function, String language) {
        this.type = type;
        this.function = function;
        this.language = normalizeLanguage(language);
    }

    public String getLanguage() {
        return normalizeLanguage(language);
    }

    public void setLanguage(String language) {
        this.language = normalizeLanguage(language);
    }

    private String normalizeLanguage(String value) {
        if (value == null || value.isBlank()) {
            return Task.LANGUAGE_JAVA;
        }
        return value;
    }
}
