package it.treis.utils.charset;

import it.treis.utils.charset.Character;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author Davide Gurgone
 */
public class CharactersEncoder {

    private static Character[] characters = new Character[8483];
    static{
        //ASCII Entities with new Entity Names
//        characters[34]   = new Character((char)34,   "quotation mark",                   "&quot;", "&#34;");
//        characters[38]   = new Character((char)38,    "ampersand",                        "&amp;", "&#38;");
//        characters[39]   = new Character((char)39,   "apostrophe",                       "&apos;", "&#39;");
//        characters[60]   = new Character((char)60,    "less-than",                        "&lt;", "&#60;");
//        characters[62]   = new Character((char)62,    "greater-than",                     "&gt;", "&#62;");

        //ISO 8859-1 Symbol Entities
    	characters[145]  = new Character((char)145,    "apostrofo",                         "'", "'" ,"'");
    	characters[146]  = new Character((char)146,    "apostrofo",                         "'", "'" ,"'");
    	characters[147]  = new Character((char)147,    "virgolette",                         "\"", "\"", "\"");
    	characters[148]  = new Character((char)148,    "virgolette",                         "\"", "\"", "\"");
    	characters[160]  = new Character((char)160,    "non-breaking space",               "&nbsp;", "&#160;");
        characters[161]  = new Character((char)161,    "inverted exclamation mark",        "&iexcl;", "&#161;");
        characters[162]  = new Character((char)162,    "cent",                             "&cent;", "&#162;", "c");
        characters[163]  = new Character((char)163,    "pound",                            "&pound;", "&#163;", "l");
        characters[164]  = new Character((char)164,    "currency",                         "&curren;", "&#164;", "_");
        characters[165]  = new Character((char)165,    "yen",                              "&yen;", "&#165;", "Y");
        characters[166]  = new Character((char)166,    "broken vertical bar",              "&brvbar;", "&#166;", "_");
        characters[167]  = new Character((char)167,    "section",                          "&sect;", "&#167;", "S");
        characters[168]  = new Character((char)168,    "spacing diaeresis",                "&uml;", "&#168;", "_");
        characters[169]  = new Character((char)169,    "copyright",                        "&copy;", "&#169;", "");
        characters[170]  = new Character((char)170,    "feminine ordinal indicator",       "&ordf;", "&#170;", "");
        characters[171]  = new Character((char)171,    "angle quotation mark (left)",      "&laquo;", "&#171;", "");
        characters[172]  = new Character((char)172,    "negation",                         "&not;", "&#172;", "");
        characters[173]  = new Character((char)173,    "soft hyphen",                      "&shy;", "&#173;", "-");
        characters[174]  = new Character((char)174,    "registered trademark",             "&reg;", "&#174;", "");
        characters[175]  = new Character((char)175,    "spacing macron",                   "&macr;", "&#175;", "");
        characters[176]  = new Character((char)176,    "degree",                           "&deg;", "&#176;", "");
        characters[177]  = new Character((char)177,    "plus-or-minus",                    "&plusmn;", "&#177;", "");
        characters[178]  = new Character((char)178,    "superscript 2",                    "&sup2;", "&#178;", "");
        characters[179]  = new Character((char)179,    "superscript 3",                    "&sup3;", "&#179;", "");
        characters[180]  = new Character((char)180,    "spacing acute",                    "&acute;", "&#180;", "");
        characters[181]  = new Character((char)181,    "micro",                            "&micro;", "&#181;", "");
        characters[182]  = new Character((char)182,    "paragraph",                        "&para;", "&#182;", "");
        characters[183]  = new Character((char)183,    "middle dot",                       "&middot;", "&#183;", "");
        characters[184]  = new Character((char)184,    "spacing cedilla",                  "&cedil;", "&#184;", "_");
        characters[185]  = new Character((char)185,    "superscript 1",                    "&sup1;", "&#185;", "");
        characters[186]  = new Character((char)186,    "masculine ordinal indicator",      "&ordm;", "&#186;", "");
        characters[187]  = new Character((char)187,    "angle quotation mark (right)",     "&raquo;", "&#187;", "");
        characters[188]  = new Character((char)188,    "fraction 1/4",                     "&frac14;", "&#188;", "");
        characters[189]  = new Character((char)189,    "fraction 1/2",                     "&frac12;", "&#189;", "");
        characters[190]  = new Character((char)190,    "fraction 3/4",                     "&frac34;", "&#190;", "");
        characters[191]  = new Character((char)191,    "inverted question mark",           "&iquest;", "&#191;", "");
        characters[215]  = new Character((char)215,    "multiplication",                   "&times;", "&#215;", "");
        characters[247]  = new Character((char)247,    "division",                         "&divide;", "&#247;", "");

        //ISO 8859-1 Character Entities
        characters[192]  = new Character((char)192,    "capital a, grave accent",          "&Agrave;", "&#192;", "A");
        characters[193]  = new Character((char)193,    "capital a, acute accent",          "&Aacute;", "&#193;", "A");
        characters[194]  = new Character((char)194,    "capital a, circumflex accent",     "&Acirc;", "&#194;", "A");
        characters[195]  = new Character((char)195,    "capital a, tilde",                 "&Atilde;", "&#195;", "A");
        characters[196]  = new Character((char)196,    "capital a, umlaut mark",           "&Auml;", "&#196;", "A");
        characters[197]  = new Character((char)197,    "capital a, ring",                  "&Aring;", "&#197;", "A");
        characters[198]  = new Character((char)198,    "capital ae",                       "&AElig;", "&#198;", "AE");
        characters[199]  = new Character((char)199,    "capital c, cedilla",               "&Ccedil;", "&#199;", "C");
        characters[200]  = new Character((char)200,    "capital e, grave accent",          "&Egrave;", "&#200;", "E");
        characters[201]  = new Character((char)201,    "capital e, acute accent",          "&Eacute;", "&#201;", "E");
        characters[202]  = new Character((char)202,    "capital e, circumflex accent",     "&Ecirc;", "&#202;", "E");
        characters[203]  = new Character((char)203,    "capital e, umlaut mark",           "&Euml;", "&#203;", "E");
        characters[204]  = new Character((char)204,    "capital i, grave accent",          "&Igrave;", "&#204;", "I");
        characters[205]  = new Character((char)205,    "capital i, acute accent",          "&Iacute;", "&#205;", "I");
        characters[206]  = new Character((char)206,    "capital i, circumflex accent",     "&Icirc;", "&#206;", "I");
        characters[207]  = new Character((char)207,    "capital i, umlaut mark",           "&Iuml;", "&#207;", "I");
        characters[208]  = new Character((char)208,    "capital eth, Icelandic",           "&ETH;", "&#208;", "D");
        characters[209]  = new Character((char)209,    "capital n, tilde",                 "&Ntilde;", "&#209;", "O");
        characters[210]  = new Character((char)210,    "capital o, grave accent",          "&Ograve;", "&#210;", "O");
        characters[211]  = new Character((char)211,    "capital o, acute accent",          "&Oacute;", "&#211;", "O");
        characters[212]  = new Character((char)212,    "capital o, circumflex accent",     "&Ocirc;", "&#212;", "O");
        characters[213]  = new Character((char)213,    "capital o, tilde",                 "&Otilde;", "&#213;", "O");
        characters[214]  = new Character((char)214,    "capital o, umlaut mark",           "&Ouml;", "&#214;", "O");
        characters[216]  = new Character((char)216,    "capital o, slash",                 "&Oslash;", "&#216;", "O");
        characters[217]  = new Character((char)217,    "capital u, grave accent",          "&Ugrave;", "&#217;", "U");
        characters[218]  = new Character((char)218,    "capital u, acute accent",          "&Uacute;", "&#218;", "U");
        characters[219]  = new Character((char)219,    "capital u, circumflex accent",     "&Ucirc;", "&#219;", "U");
        characters[220]  = new Character((char)220,    "capital u, umlaut mark",           "&Uuml;", "&#220;", "U");
        characters[221]  = new Character((char)221,    "capital y, acute accent",          "&Yacute;", "&#221;", "Y");
        characters[222]  = new Character((char)222,    "capital THORN, Icelandic",         "&THORN;", "&#222;", "T");
        characters[223]  = new Character((char)223,    "small sharp s, German",            "&szlig;", "&#223;", "b");
        characters[224]  = new Character((char)224,    "small a, grave accent",            "&agrave;", "&#224;", "a");
        characters[225]  = new Character((char)225,    "small a, acute accent",            "&aacute;", "&#225;", "a");
        characters[226]  = new Character((char)226,    "small a, circumflex accent",       "&acirc;", "&#226;", "a");
        characters[227]  = new Character((char)227,    "small a, tilde",                   "&atilde;", "&#227;", "a");
        characters[228]  = new Character((char)228,    "small a, umlaut mark",             "&auml;", "&#228;", "a");
        characters[229]  = new Character((char)229,    "small a, ring",                    "&aring;", "&#229;", "a");
        characters[230]  = new Character((char)230,    "small ae",                         "&aelig;", "&#230;", "ae");
        characters[231]  = new Character((char)231,    "small c, cedilla",                 "&ccedil;", "&#231;", "c");
        characters[232]  = new Character((char)232,    "small e, grave accent",            "&egrave;", "&#232;", "e");
        characters[233]  = new Character((char)233,    "small e, acute accent",            "&eacute;", "&#233;", "e");
        characters[234]  = new Character((char)234,    "small e, circumflex accent",       "&ecirc;", "&#234;", "e");
        characters[235]  = new Character((char)235,    "small e, umlaut mark",             "&euml;", "&#235;", "e");
        characters[236]  = new Character((char)236,    "small i, grave accent",            "&igrave;", "&#236;", "i");
        characters[237]  = new Character((char)237,    "small i, acute accent",            "&iacute;", "&#237;", "i");
        characters[238]  = new Character((char)238,    "small i, circumflex accent",       "&icirc;", "&#238;", "i");
        characters[239]  = new Character((char)239,    "small i, umlaut mark",             "&iuml;", "&#239;", "i");
        characters[240]  = new Character((char)240,    "small eth, Icelandic",             "&eth;", "&#240;", "e");
        characters[241]  = new Character((char)241,    "small n, tilde",                   "&ntilde;", "&#241;", "n");
        characters[242]  = new Character((char)242,    "small o, grave accent",            "&ograve;", "&#242;", "o");
        characters[243]  = new Character((char)243,    "small o, acute accent",            "&oacute;", "&#243;", "o");
        characters[244]  = new Character((char)244,    "small o, circumflex accent",       "&ocirc;", "&#244;", "o");
        characters[245]  = new Character((char)245,    "small o, tilde",                   "&otilde;", "&#245;", "o");
        characters[246]  = new Character((char)246,    "small o, umlaut mark",             "&ouml;", "&#246;", "o");
        characters[248]  = new Character((char)248,    "small o, slash",                   "&oslash;", "&#248;", "o");
        characters[249]  = new Character((char)249,    "small u, grave accent",            "&ugrave;", "&#249;", "u");
        characters[250]  = new Character((char)250,    "small u, acute accent",            "&uacute", "&#250;", "u");
        characters[251]  = new Character((char)251,    "small u, circumflex accent",       "&ucirc;", "&#251;", "u");
        characters[252]  = new Character((char)252,    "small u, umlaut mark",             "&uuml;", "&#252;", "u");
        characters[253]  = new Character((char)253,    "small y, acute accent",            "&yacute;", "&#253;", "y");
        characters[254]  = new Character((char)254,    "small thorn, Icelandic",           "&thorn;", "&#254;", "y");
        characters[255]  = new Character((char)255,    "small y, umlaut mark",             "&yuml;", "&#255;", "y");

        //Some Other Entities supported by HTML 
        characters[338]  = new Character((char)338,    "capital ligature OE",              "&OElig;", "&#338;", "OE");
        characters[339]  = new Character((char)339,    "small ligature oe",                "&oelig;", "&#339;", "oe");
        characters[352]  = new Character((char)352,    "capital S with caron",             "&Scaron;", "&#352;", "S");
        characters[353]  = new Character((char)353,    "small S with caron",               "&scaron;", "&#353;", "s");
        characters[376]  = new Character((char)376,    "capital Y with diaeres",           "&Yuml;", "&#376;", "Y");
        characters[710]  = new Character((char)710,    "modifier letter circumflex accent","&circ;", "&#710;", "");
        characters[732]  = new Character((char)732,    "small tilde",                      "&tilde;", "&#732;", "");
        characters[8194] = new Character((char)8194,    "en space",                        "&ensp;", "&#8194;", "");
        characters[8195] = new Character((char)8195,    "em space",                        "&emsp;", "&#8195;", "");
        characters[8201] = new Character((char)8201,    "thin space",                      "&thinsp;", "&#8201;", "");
        characters[8204] = new Character((char)8204,  "zero width non-joiner",      "&zwnj;", "&#8204;", "0");
        characters[8205] = new Character((char)8205,  "zero width joiner",          "&zwj;", "&#8205;", "0");
        characters[8206] = new Character((char)8206,  "left-to-right mark",         "&lrm;", "&#8206;", "");
        characters[8207] = new Character((char)8207,  "right-to-left mark",         "&rlm;", "&#8207;", "");
        characters[8211] = new Character((char)8211,    "en dash",                         "&ndash;", "&#8211;", "");
        characters[8212] = new Character((char)8212,    "em dash",                         "&mdash;", "&#8212;", "");
        characters[8216] = new Character((char)8216,    "left single quotation mark",      "&lsquo;", "&#8216;", "");
        characters[8217] = new Character((char)8217,    "right single quotation mark",     "&rsquo;", "&#8217;", "");
        characters[8218] = new Character((char)8218,    "single low-9 quotation mark",     "&sbquo;", "&#8218;", "");
        characters[8220] = new Character((char)8220,    "left double quotation mark",      "&ldquo;", "&#8220;", "");
        characters[8221] = new Character((char)8221,    "right double quotation mark",     "&rdquo;", "&#8221;", "");
        characters[8222] = new Character((char)8222,    "double low-9 quotation mark",     "&bdquo;", "&#8222;", "");
        characters[8224] = new Character((char)8224,    "dagger",                          "&dagger;", "&#8224;", "");
        characters[8225] = new Character((char)8225,    "double dagger",                   "&Dagger;", "&#8225;");
        characters[8230] = new Character((char)8230,    "horizontal ellipsis",             "&hellip;", "&#8230;");
        characters[8240] = new Character((char)8240,    "per mille",                       "&permil;", "&#8240;");
        characters[8249] = new Character((char)8249,    "single left-pointing angle quotation", "&lsaquo;", "&#8249;");
        characters[8250] = new Character((char)8250,    "single right-pointing angle quotation", "&rsaquo;", "&#8250;");
        characters[8364] = new Character((char)8364,    "euro",                             "&euro;", "&#8364;", "");
        characters[8482] = new Character((char)8482,    "trademark",                        "&trade;", "&#8482;", "tm");
   }

    public static String toHtmlEntityCode(String s){
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i<s.length(); i++){
            char c = s.charAt(i);
            if(characters[c]==null){
                sb.append(c);
            } else {
                sb.append(characters[c].getEntity());
            }
        }
        return sb.toString();
    }

    public static String toHtmlEscapeChar(String s){
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i<s.length(); i++){
            char c = s.charAt(i);
            if(characters[c]==null){
                sb.append(c);
            } else {
                sb.append(characters[c].getEscape());
            }
        }
        return sb.toString();
    }
    
    public static String toFileEscapeChar(String s){
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i<s.length(); i++){
        	try{
	            char c = s.charAt(i);
	            if(characters[c]==null){
	                sb.append(c);
	            } else {
	                sb.append(characters[c].getAscii());
	            }
        	}catch(Exception e){
        		continue;
        	}
        }
        return sb.toString();
    }

    /**
	 * Decodifica da stringa HTML encoded a testo.
	 *
	 * <P>Si suppone che i caratteri da 128 a 255 siano espressi nella forma &#34;#codice_ascii;
	 * <P>Decodifica una stringa contenente caratteri speciali HTML nella forma:
	 * <UL>
	 * 	<LI><CODE>&amp;quot;</CODE>
	 * 	<LI><CODE>&&amp;amp;</CODE>
	 * 	<LI><CODE>&amp;gt;</CODE>
	 * 	<LI><CODE>&amp;lt;</CODE>
	 * 	<LI><CODE>&amp;#codice_ascii</CODE>
	 * </UL>
	 * <P>nei relativi caratteri ascii.
	 *
	 * <P>N.B. il carattere ";" finale e' ritenuto opzionale
	 *
	 * @param strValue La stringa contenente le HTML entities.
	 * @return La stringa ASCII con gli HTML entities decodificati.
	 * @see htmlEncode(String)
	 */
	public static String htmlDecode(String strValue) {
		if(strValue == null) return null;
		
		StringBuffer strbuf = new StringBuffer();
		String subs = "";

		int len = strValue.length();
		char c;
		boolean bSpecialCar = false;

		for (int pos = 0; pos < len; pos++) {
			c = strValue.charAt(pos);
			if ((c == ';') && bSpecialCar) {
				continue;
			}

			bSpecialCar = false;
			if (c == '&') {
				if (pos + 4 < len) {
					subs = strValue.substring(pos + 1, pos + 5);
				} else if (pos + 3 < len) {
					subs = strValue.substring(pos + 1, pos + 4);
				} else if (pos + 2 < len) {
					subs = strValue.substring(pos + 1, pos + 3);
				}

				if (subs.startsWith("quot")) {
					strbuf.append("\"");
					pos += 4;
					bSpecialCar = true;

				} else if (subs.startsWith("amp")) {
					strbuf.append("&");
					pos += 3;
					bSpecialCar = true;

				} else if (subs.startsWith("lt")) {
					strbuf.append("<");
					pos += 2;
					bSpecialCar = true;

				} else if (subs.startsWith("gt")) {
					strbuf.append(">");
					pos += 2;
					bSpecialCar = true;

				} else if (subs.startsWith("#")) {
					int ascii = 0;
					try {
						if(subs.charAt(3) == ';') {
							ascii = Integer.parseInt(subs.substring(1, 3));
						} else if(subs.charAt(4) == ';') {
							ascii = Integer.parseInt(subs.substring(1, 4));
						}
					} catch (Exception e) {}

					if (ascii >= 128) {
						strbuf.append((char) ascii);
						pos += 4;
						bSpecialCar = true;

					} else if (ascii == 35
								|| ascii == 34
								|| ascii == 38
								|| ascii == 60
								|| ascii == 62)
					{
						strbuf.append((char) ascii);
						pos += 3;
						bSpecialCar = true;

					} else if (ascii == 13) {
						strbuf.append("\n");
						pos += 3;
						bSpecialCar = true;
					}
				}
			}
			if (!bSpecialCar) {
				strbuf.append(c);
			}
		}
		return strbuf.toString();
	}

	/**
	 * Codifica una stringa di testo codificando le entities HTML.
	 *
	 * <P>I caratteri da 128 a 255 vengono espressi nella forma &#34;#codice_ascii;
	 * <P>Codifica una stringa di testo sostituendo ai caratteri che necessitano codifica la
	 * codifica relativa come HTML entity.
	 * <P>Sono supportati i seguenti:
	 * <UL>
	 * 	<LI><CODE>&#34;quot;</CODE>
	 * 	<LI><CODE>&#34;amp;</CODE>
	 * 	<LI><CODE>&#34;gt;</CODE>
	 * 	<LI><CODE>&#34;lt;</CODE>
	 * 	<LI><CODE>&#34;#codice_ascii;</CODE> per tutti i caratteri con ASII &gt; 128
	 * </UL>
	 *
	 * @param strValue La stringa contenente le HTML entities.
	 * @return La stringa ASCII con gli HTML entities decodificati.
	 * @see htmlDecode(String)
	 */
	public static String htmlEncode(String strValue) {
		if(strValue == null) return "";
		
		StringBuffer strbuf = new StringBuffer();

		int len = strValue.length();
		char c;

		for (int pos = 0; pos < len; pos++) {
			c = strValue.charAt(pos);
			if (c == 133) { // " to &#13;
				strbuf.append("&#13;");

			} else if (c >= 128) { //viene scritto nella forma &#codice_ascii;
				strbuf.append("&#");
				strbuf.append(Integer.toString(c));
				strbuf.append(";");

			} else if (c == 34) { // " to &quot;
				strbuf.append("&quot;");

			} else if (c == 38) { // & to &amp;
				strbuf.append("&amp;");

			} else if (c == 60) { // < to &lt;
				strbuf.append("&lt;");

			} else if (c == 62) { // > to &gt;
				strbuf.append("&gt;");

			} else {
				strbuf.append(c);
			}
		}
		return strbuf.toString();
	}

	/**
	 * Decodifica una stringa contenente caratteri URLencoded.
	 *
	 * <P>I caratteri da 'a' a 'z', da 'A' a 'Z' e da '0' a '9'
	 * rimangono gli stessi.
	 * <P>Il segno '+' e' convertito in 'spazio'
	 * <P>I caratteri restanti sono convertiti in una stringa che inizia
	 * con il segno di percentuale '%' e la rappresentazione esadecimale
	 * del codice ascii.
	 * <P>es. ascii = 60, URL = %3C
	 *
	 * @param strValue URL da decodificare.
	 * @return URL decodificato.
	 * @see urlEncode(String, java.util.HashMap)
	 */
	public static String urlDecode(String strValue) {
		//URLDecoder _oURLDecoder = new URLDecoder();
		String decodifica = "";

		try {
			decodifica = URLDecoder.decode(strValue, "UTF-8");
		} catch (java.lang.Exception e) {
			System.out.println(e.getMessage());
		}
		return decodifica;
	}

	/**
	 * Decodifica una stringa contenente l'URL i caratteri da 'a' a 'z',
	 * da 'A' a 'Z' e da '0' a '9' rimangono gli stessi.<BR>
	 * Il segno '+' e' convertito in 'spazio' i caratteri restanti sono
	 * convertiti in una stringa che inizia con il segno di percentuale '%'
	 * e la rappresentazione esadecimale del codice ascii.<BR>
	 * es. ascii = 60, URL = %3C
	 * Se non nullo verra' considerato anche un set di parametri separati
	 * dall'URL con '?' e tra loro da '&'.<BR>
	 * Le coppie chiave parametro dovranno essere necessariamente delle
	 * stringhe che verranno encodate come URL e separate da '='.<BR>
	 * (<B>ATTENZIONE!</B> Se l'indirizzo contiene l'indicazione di porta - es.: localhost:8080 -
	 * verra' tradotto anche il ':' - cioe': localhost%3A8080 -)
	 *
	 * @param url URL da codificare.
	 * @param params Parametri da accodare.
	 * @return URL codificato.
	 * @see urlDecode(String)
	 */
	@SuppressWarnings("unchecked")
	public static String urlEncode(String url, HashMap params) throws UnsupportedEncodingException {
		boolean http = false;
		if (url.startsWith("http://")) {
			http = true;
			url = url.substring(7);
		}
		java.util.StringTokenizer tok = new java.util.StringTokenizer(url, "/");
		url = http ? "http://" : "";
		while (tok.hasMoreTokens()) {
			url += URLEncoder.encode(tok.nextToken(), "UTF-8");
			if (tok.hasMoreTokens()) {
				url += "/";
			}
		}
		if (params != null && !params.isEmpty()) {
			url += "?";
			Iterator iteretor = params.keySet().iterator();
			while (iteretor.hasNext()) {
				String att = (String) iteretor.next();
				url += URLEncoder.encode(att, "UTF-8")
					+ "="
					+ URLEncoder.encode((String) params.get(att), "UTF-8");
				if (iteretor.hasNext()) {
					url += "&";
				}
			}
		}
		return url;
	}
}
