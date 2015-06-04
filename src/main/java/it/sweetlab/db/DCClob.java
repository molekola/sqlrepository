package it.sweetlab.db;

public class DCClob {


    private final String str;

    /**
     * Costruttore della classe che imposta il valore
     * alla variabile privata str
	 * @param str
     */
    public DCClob(String str) {
        this.str = str;
    }

    /**
     * Metodo che restituisce la stringa
     * impostata nel costruttore della classe
     * @return la stringa
     */
    public String getString() {
        return str;
    }

	public char[] getCharArray() {
		if (str == null) return null;
		return str.toCharArray();
	}
}