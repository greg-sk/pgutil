package pgutil

import (
    "fmt"
    "strings"
	"database/sql"
	_ "github.com/lib/pq"
)

func OpenDb(dbname string) (*sql.DB, error) {
	//return sql.Open("postgres", "user=postgres host=localhost password=postgres sslmode=disable dbname=" + dbname)
	//return sql.Open("postgres", "user=root host=localhost password=root sslmode=disable dbname=" + dbname)
    // The default configuration of PostgreSQL allows "local" access to a database only to database users which have the same name as the OS user. 
    // "local" means without any IP traffic using only the Unix-Domain sockets. 
    // See the documentation for the configuration file pg_hba.conf especially the lines starting with "local" and the authentication method "peer" or "ident".
    // It's easier to create new Postgres role e.g. for "root":
    // # sudo -i -u postgres
    // # createuser -s root
    // # <ctrl> + D to go back to root login
    // # psql postgres  # now it will be logged in as root but we need to open "postgres" database
    // > \password root   # change password
    // Another option is to set "trust" in pg_hba.conf:
    //# "local" is for Unix domain socket connections only
    //local   all             all                                     trust
	return sql.Open("postgres", "host=/var/run/postgresql sslmode=disable dbname=" + dbname)
}


func DropDb(dbname string) error {
    db, err := OpenDb("postgres")
	if err != nil {
		return err
	}
	defer db.Close()
    _ , err = db.Exec("drop database " + dbname)
    if err != nil {
        fmt.Printf("ERR %v\n", err)
        return err
    }
    return nil
}

// Create Db if not exists
// Open existing db otherwise
func CreateDb(dbname string) (*sql.DB, error) {
	var db *sql.DB
	var err error

    db, err = OpenDb("postgres")
	if err != nil {
		return db, err
	}
	defer db.Close()

    _, err = db.Exec("create database " + dbname)
    if err != nil {
        //Do not return, it's OK, database exists
        fmt.Printf("%v\n", err)
    } else {
        fmt.Printf("%s database created\n", dbname)
    }

    return OpenDb(dbname)
}

func AsLine(t []string) string {
    return strings.Join(t, "\t")
}


func AsTable(t [][]string) (res string) {
    for _, r := range t {
        res += AsLine(r) + "\n"
    }
    return res
}


func GetRows(db *sql.DB, query string, args ...interface{}) ([][]string, error) {
    var res [][]string
    rows, err := db.Query(query, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    colNames, err := rows.Columns()
    for rows.Next() {
        //see http://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
        //why do I have to fight with the type system again?
        readCols := make([]interface{}, len(colNames))
        writeCols := make([]string, len(colNames))
        for i, _ := range writeCols {
            readCols[i] = &writeCols[i]
        }
        if err := rows.Scan(readCols...); err != nil {
            return nil, err
        }
        res = append(res, writeCols)
    }
    return res, nil
}

func GetRow(db *sql.DB, query string, args ...interface{}) ([]string, error) {
    ss, err := GetRows(db, query, args...)
    if err != nil {
        return nil, err
    }
    if len(ss) == 0 {
        return []string{}, nil
    } else if len(ss) == 1 {
        return ss[0], nil
    } else {
        return nil, fmt.Errorf("GetRow error: fetched %d results, expected 0 or 1", len(ss))
    }
}

func GetColumn(db *sql.DB, query string, args ...interface{}) ([]string, error) {
    ss, err := GetRows(db, query, args...)
    if err != nil {
        return nil, err
    }
    if len(ss) == 0 {
        return []string{}, nil
    } else if len(ss[0]) != 1 {
        return nil, fmt.Errorf("GetColumn error: query returns %d columns, expected 1", len(ss[0]))
    } else {
        var res []string
        for _, row := range ss {
            res = append(res, row[0])
        }
        return res, nil
    }
}

func GetValue(db *sql.DB, query string, args ...interface{}) (string, error) {
    s, err := GetRow(db, query, args...)
    if err != nil {
        return "", err
    }
    if len(s) != 1 {
        return "", fmt.Errorf("GetValue error: query returned %d items, expected 1", len(s))
    }
    return s[0], nil
}

func TableIndices(db *sql.DB, tableName string) ([]string, error) {
    indices, err := GetColumn(db, `SELECT relname FROM pg_class WHERE oid IN ( 
        SELECT indexrelid FROM pg_index, pg_class
        WHERE pg_class.relname=$1
        AND pg_class.oid=pg_index.indrelid
        AND indisunique != 't'
        AND indisprimary != 't')`, tableName)
    if err != nil {
        return nil, err
    }
    return indices, nil
}

func TableExists(db *sql.DB, tableName string) (bool, error) {
    v, err := GetValue(db, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1", tableName)
    if err != nil {
        return false, err
    }
    if v != "0" && v != "1" {
        return false, fmt.Errorf("pgTableExists error: query returned %s, expected '0' or '1'", v)
    }
    return v == "1", nil
}

func GetTables(db *sql.DB) ([]string, error) {
    return GetColumn(db, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
}


func CopyFromCsv(db *sql.DB, tableName string, csvname string) (sql.Result, error) {
    return db.Exec("COPY " + tableName + " FROM '" + csvname + "' WITH CSV")
}

