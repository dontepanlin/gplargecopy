package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Args struct {
	SrcDB     DBConfig
	DstDB     DBConfig
	BatchSize int
	AllTables bool
}

type DBConfig struct {
	URL      string
	User     string
	Password string
	Name     string
	Table    string
}

type Endpoint struct {
	token    string
	endpoint string
	host     string
	port     string
}

type Tuple map[string][]*Endpoint

var counter int32

var pool *pgxpool.Pool
var stats sync.Map
var tableLock map[string]*sync.Mutex

func main() {
	args := parseArgs()
	var err error

	pgx.Connect(context.Background(), fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable application_name=large", args.DstDB.URL, args.DstDB.Name, args.DstDB.User, args.DstDB.Password))
	pool, err = pgxpool.New(context.Background(), fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable application_name=large", args.DstDB.URL, args.DstDB.Name, args.DstDB.User, args.DstDB.Password))
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	done := make(chan bool)
	go func() {
		defer wg.Done()
		mainThread(ctx, args, &done)
	}()

	<-done
	fmt.Println("Waiting parallel cursors created...")
	time.Sleep(time.Duration(5) * time.Second)

	endpointMap := make(map[string]*Endpoint, 0)
	endpointMapDst := make(map[string]string, 0)
	tableLock = make(map[string]*sync.Mutex, 0)

	for dst_table, endpoints := range getEndpoints(ctx, args) {
		tableLock[dst_table] = &sync.Mutex{}
		for _, endpoint := range endpoints {
			endpointMap[endpoint.endpoint] = endpoint
			endpointMapDst[endpoint.endpoint] = dst_table
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker(ctx, args, dst_table, endpoint)
			}()
		}
	}

	wg.Wait()
	cancel()

	fmt.Printf("Summary:\n")
	stats.Range(func(key, value any) bool {
		fmt.Printf("%s: %d\n", key, *value.(*uint64))
		return true
	})

}

func parseArgs() Args {
	srcURL := flag.String("db-url-src", os.Getenv("DB_URL_SRC"), "Source DB URL")
	srcUser := flag.String("db-user-src", os.Getenv("DB_USER_SRC"), "Source DB User")
	srcPassword := flag.String("db-password-src", os.Getenv("DB_PASSWORD_SRC"), "Source DB Password")
	srcName := flag.String("db-name-src", os.Getenv("DB_NAME_SRC"), "Source DB Name")
	srcTable := flag.String("db-table-src", os.Getenv("DB_TABLE_SRC"), "Source DB Table")
	dstURL := flag.String("db-url-dst", os.Getenv("DB_URL_DST"), "Destination DB URL")
	dstUser := flag.String("db-user-dst", os.Getenv("DB_USER_DST"), "Destination DB User")
	dstPassword := flag.String("db-password-dst", os.Getenv("DB_PASSWORD_DST"), "Destination DB Password")
	dstName := flag.String("db-name-dst", os.Getenv("DB_NAME_DST"), "Destination DB Name")
	dstTable := flag.String("db-table-dst", os.Getenv("DB_TABLE_DST"), "Destination DB Table")
	batchSize := flag.Int("batch", 10000, "Batch size")
	allTables := flag.Bool("all", false, "Process all tables")

	flag.Parse()

	return Args{
		SrcDB: DBConfig{
			URL:      *srcURL,
			User:     *srcUser,
			Password: *srcPassword,
			Name:     *srcName,
			Table:    *srcTable,
		},
		DstDB: DBConfig{
			URL:      *dstURL,
			User:     *dstUser,
			Password: *dstPassword,
			Name:     *dstName,
			Table:    *dstTable,
		},
		BatchSize: *batchSize,
		AllTables: *allTables,
	}
}

func mainThread(ctx context.Context, args Args, done *chan bool) {
	db, err := pgx.Connect(ctx, fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable application_name=large", args.SrcDB.URL, args.SrcDB.Name, args.SrcDB.User, args.SrcDB.Password))
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		panic(err)
	}
	defer tx.Commit(ctx)

	prcs := make([]string, 0, 0)

	if args.AllTables {
		tables := getAllTables(ctx, tx)
		for _, table := range tables {
			prc, err := declareParCursor(ctx, tx, table, table)
			if err != nil {
				panic(err)
			}
			prcs = append(prcs, prc)
		}
	} else {
		prc, err := declareParCursor(ctx, tx, args.SrcDB.Table, args.DstDB.Table)
		if err != nil {
			panic(err)
		}
		prcs = append(prcs, prc)
	}
	*done <- true
	for _, prc := range prcs {
		query := fmt.Sprintf("SELECT gp_wait_parallel_retrieve_cursor( '%s', 600);", prc)
		tx.Exec(ctx, query)
	}
}

func getEndpoints(ctx context.Context, args Args) Tuple {
	db, err := pgx.Connect(ctx, fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable application_name=large", args.SrcDB.URL, args.SrcDB.Name, args.SrcDB.User, args.SrcDB.Password))
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		panic(err)
	}
	defer tx.Commit(ctx)

	var allTables []string
	if args.AllTables {
		allTables = getAllTables(ctx, tx)
	} else {
		allTables = []string{args.DstDB.Table}
	}

	normalizedTables := make(map[string]string, 0)
	for _, val := range allTables {
		normalizedTables[tableNormalize(val)] = val
	}

	rows, err := tx.Query(ctx, "SELECT cursorname, auth_token, endpointname, hostname, port FROM gp_get_endpoints() order by cursorname;")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	result := make(Tuple, 0)
	for rows.Next() {
		var key string
		value := Endpoint{}
		err = rows.Scan(&key, &value.token, &value.endpoint, &value.host, &value.port)
		if err != nil {
			panic(err)
		}
		key = normalizedTables[key]
		if _, exists := result[key]; !exists {
			result[key] = make([]*Endpoint, 0, 1)
		}
		result[key] = append(result[key], &value)
	}
	return result

}

func worker(ctx context.Context, args Args, dst_table string, endpoint *Endpoint) {
	db, err := pgx.Connect(ctx, fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable application_name=large options='-c gp_retrieve_conn=true'", endpoint.host, endpoint.port, args.SrcDB.Name, args.SrcDB.User, endpoint.token))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close(ctx)

	query := fmt.Sprintf("retrieve %d from endpoint %s", args.BatchSize, endpoint.endpoint)
	count := uint64(0)

	for {
		rows, err := db.Query(ctx, query)
		if err != nil {
			fmt.Println(err)
			return
		}
		columns := make([]string, 0, len(rows.FieldDescriptions()))
		for _, field := range rows.FieldDescriptions() {
			columns = append(columns, field.Name)
		}

		values := make([][]any, 0, 0)

		loaded := uint64(0)

		for rows.Next() {
			atomic.AddInt32(&counter, 1)
			loaded++
			nextValues, err := rows.Values()
			if err != nil {
				fmt.Println(err)
				return
			}
			values = append(values, nextValues)
		}
		rows.Close()
		if count == (count + loaded) {
			break
		}

		if err := load(ctx, args, dst_table, endpoint, columns, values); err != nil {
			fmt.Println(err)
			return
		}
		count += loaded
	}
	fmt.Printf("Copied table %s from endpoint %s with %d rows\n", dst_table, endpoint.endpoint, count)
	value, _ := stats.LoadOrStore(dst_table, new(uint64))
	atomic.AddUint64(value.(*uint64), count)
}

func getAllTables(ctx context.Context, db pgx.Tx) (result []string) {
	rows, err := db.Query(ctx, `
	SELECT table_schema || '.' || table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
	`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			panic(err)
		}
		result = append(result, table)
	}
	return
}

func tableNormalize(table string) string {
	return strings.ToLower(strings.ReplaceAll(table, ".", "_"))
}

func declareParCursor(ctx context.Context, tx pgx.Tx, table string, dst_table string) (string, error) {
	parName := tableNormalize(dst_table)
	query := fmt.Sprintf("DECLARE %s PARALLEL RETRIEVE CURSOR FOR SELECT * FROM %s;", parName, table)
	_, err := tx.Exec(ctx, query)
	if err != nil {
		return "", err
	}
	return parName, nil
}

func load(ctx context.Context, _ Args, dst_table string, endpoint *Endpoint, columns []string, data [][]any) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Commit(ctx)

	tmpTableName := endpoint.endpoint + "_" + endpoint.host + "_" + endpoint.port

	if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (like %s);", tmpTableName, dst_table)); err != nil {
		return err
	}

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{tmpTableName}, columns, pgx.CopyFromRows(data)); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s select * from %s on conflict do nothing;", dst_table, tmpTableName)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("DROP TABLE %s;", tmpTableName)); err != nil {
		return err
	}

	return nil
}
