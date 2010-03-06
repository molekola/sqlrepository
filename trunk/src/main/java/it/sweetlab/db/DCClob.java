package it.sweetlab.db;

public class DCClob {


    private String str;

    /**
     * Costruttore della classe che imposta il valore
     * alla variabile privata str
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

}