/*
 * EscapeCharacter.java
 *
 * Created on 21 novembre 2006, 10.32
 *
 */

package it.treis.utils.charset;

/**
 *
 * @author Davide Gurgone
 */
class Character {

    /** Holds value of property character. */
    private char character;

    /** Holds value of property escape. */
    private String escape;

    /** Holds value of property description. */
    private String description;

    private String ascii;
    
    
    /** Creates a new instance of EscapeCharacter, uses empty string for ascii representation */
    public Character(char character, String description, String entity, String escape) {
        this(character, description, entity, escape, "");
    }

    /** Creates a new instance of EscapeCharacter */
    public Character(char character, String description, String entity, String escape, String ascii) {
    	this.character = character;
    	this.description = description;
    	this.entity = entity;
    	this.escape = escape;
    	this.ascii = ascii;
    }

    public char getCharacter() {
        return this.character;
    }

    public void setCharacter(char character) {
        this.character = character;
    }

    public String getEscape() {
        return this.escape;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    private String entity;

    public String getEntity() {
        return this.entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

	public Object getAscii() {
		return this.ascii;
	}

	public void setAscii(String ascii) {
		this.ascii = ascii;
	}
}
