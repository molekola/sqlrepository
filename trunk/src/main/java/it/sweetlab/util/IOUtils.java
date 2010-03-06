package it.sweetlab.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IOUtils {

  private static final Log logger = LogFactory.getLog(IOUtils.class);

	public static void fileCopy(String orig, String dest) throws IOException {
		BufferedInputStream is = null;
		BufferedOutputStream os = null;
		try {
			is = new BufferedInputStream(new FileInputStream(orig));
			os = new BufferedOutputStream(new FileOutputStream(dest));
			int c;
			while ((c = is.read()) != -1)
				os.write(c);
		} finally {
			// Chiude gli streams se necessario
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
					logger.warn("Impossibile chiudere l'input stream", e);
				}
	
			if (os != null)
				try {
					os.close();
				} catch (IOException e) {
					logger.warn("Impossibile chiudere l'output stream", e);
				}
		}
	}

	/**
	 * Dato il percorso di un file, ne restituisce il contenuto.
	 * */
	public static String getFileContent(String s) {
		return getFileContent(new File(s));
	}

	/**
	 * Dato un File f, ne restituisce il contenuto. 
	 * */
	public static String getFileContent(File f){
		BufferedInputStream is = null;
		StringBuffer sb = new StringBuffer();
		try {
			is = new BufferedInputStream(new FileInputStream(f));
			int c;
			while ((c=is.read())!=-1) sb.append((char)c);
		} catch (IOException e) {
			logger.warn("File not found",e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				logger.warn("Impossibile chiudere l'output stream",e);
			}
		}
		return sb.toString();
	}
	
	public static String getExtension(String fileName){
		return fileName.substring(fileName.lastIndexOf('.'));
	}
	public static String toString(InputStream is){
		if (is==null) return "";
		StringBuffer sb = new StringBuffer();
		try {
			int c;
			while ((c=is.read())!=-1) sb.append((char)c);
		} catch (IOException e) {
			logger.warn("File not found",e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				logger.warn("Impossibile chiudere l'output stream",e);
			}
		}
		return sb.toString();	
	}
	
	/**Extract class file at runtime. */
	public static String classLocation( Class cls ) {
		if( cls == null )
			return null ;
		String name = cls.getName().replace( '.' , '/' ) ;
		URL loc = cls.getResource( "/" + name + ".class" ) ;
		File f = new File( loc.getFile() ) ;
		// Class file is inside a jar file.
		if( f.getPath().startsWith( "file:" ) ) {
			String s = f.getPath() ;
			int index = s.indexOf( '!' ) ;
			// It confirm it is a jar file
			if( index != -1 ) {
				f = new File( s.substring( 5 ).replace( '!' , File.separatorChar ) ) ;
				return f.getPath() ;
			}
		}
		try {
			f = f.getCanonicalFile() ;
		}catch( IOException ioe ) {
			ioe.printStackTrace() ;
			return null ;
		}
		return f.getPath() ;
	}	
	
	/** Extracts classes location */
	public static String getResourceLocation() {
		return getResourceLocation(null);
	}
	
	/** Extracts location of a specific resource location*/
	public static String getResourceLocation(String rs) {
		if (rs == null) rs = "/";
		URL loc = IOUtils.class.getResource( rs ) ;
		File f = new File( loc.getFile() ) ;
		// Class file is inside a jar file.
		if( f.getPath().startsWith( "file:" ) ) {
			String s = f.getPath() ;
			int index = s.indexOf( '!' ) ;
			// It confirm it is a jar file
			if( index != -1 ) {
				f = new File( s.substring( 5 ).replace( '!' , File.separatorChar ) ) ;
				return f.getPath() ;
			}
		}
		try {
			f = f.getCanonicalFile() ;
		}catch( IOException ioe ) {
			ioe.printStackTrace() ;
			return null ;
		}
		return f.getPath() ;
	}	
		
	public static void main(String[] args) {
		System.out.println( getResourceLocation( null ) );
		System.out.println( getResourceLocation( "/" ) );
	}	
}