package it.sweetlab.db;

public class DCBlob {

    private byte[] b;

    /**
     * Costruttore della classe che imposta il valore passato in input
     * alla variabile privata b
     */

    public DCBlob(byte[] b) {
        this.b = b;
    }

    /**
     * Costruttore della classe converte la stringa in byte utilizzando il metodo getBytes della stringa
     * e imposta tale valore alla variabile b
     */
    public DCBlob(String str) {
        this.b = str.getBytes();
    }

    /**
     * Metodo che  restituisce i byte impostati
     * @return l'array di byte
     */
    public byte[] getBytes() {
        return b;
    }

}