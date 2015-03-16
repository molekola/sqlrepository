Simple library born in 2003 to separate SQL Files to java code.

It wraps JDBC API and allowed developers to write SQL queries like in commercial DB IDE.

## Java Code Example: ##

---

```
/** Extracts Menu Items 
 * @param name the menu name 
 * @throws java.sql.SQLException */
public static List<Map<String, Object>> getMenu(String name) throws SQLException {

	// Instantiate Library
	DataLink dl = new DataLink();

	// Prepares Parameters
	Map<String, String> params = new HashMap<String, String>();
	params.put("NAME", name);

	try {

		// Execute SQL Query named SelectNavMenu.sql, in IndexService class path
		return dl.execQuery(DataModule.getQuery(IndexService.class,	"SelectMenu", params));

	} finally {

		// Release Connection
		dl.release();

	}
}
```

## SelectMenu.sql - SQL Code Example ##

---

```
SELECT	ORDER,
		HREF,
		LABEL,
		TARGET
  FROM	MENU
 WHERE  NAME = :NAME
 ORDER  BY ORDER
```