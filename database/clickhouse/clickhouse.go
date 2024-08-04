package clickhouse

import (
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
)

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMigrationsTableEngine = "TinyLog"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName          string
	ClusterName           string
	MigrationsTable       string
	MigrationsTableEngine string
	MultiStatementEnabled bool
	MultiStatementMaxSize int
}

func init() {
	database.Register("clickhouse", &ClickHouse{})
}

func WithInstance(conn *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	ch := &ClickHouse{
		conn:   conn,
		config: config,
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

type ClickHouse struct {
	conn     *sql.DB
	config   *Config
	isLocked atomic.Bool
	url      *url.URL
	addrs    []string
	addrsMux sync.Mutex
}

func (ch *ClickHouse) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	q := migrate.FilterCustomQuery(purl)
	q.Scheme = "tcp"
	conn, err := sql.Open("clickhouse", q.String())
	if err != nil {
		return nil, err
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	migrationsTableEngine := DefaultMigrationsTableEngine
	if s := purl.Query().Get("x-migrations-table-engine"); len(s) > 0 {
		migrationsTableEngine = s
	}

	ch = &ClickHouse{
		conn: conn,
		config: &Config{
			MigrationsTable:       purl.Query().Get("x-migrations-table"),
			MigrationsTableEngine: migrationsTableEngine,
			DatabaseName:          purl.Query().Get("database"),
			ClusterName:           purl.Query().Get("x-cluster-name"),
			MultiStatementEnabled: purl.Query().Get("x-multi-statement") == "true",
			MultiStatementMaxSize: multiStatementMaxSize,
		},
		url: q,
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

func (ch *ClickHouse) init() error {
	if len(ch.config.DatabaseName) == 0 {
		if err := ch.conn.QueryRow("SELECT currentDatabase()").Scan(&ch.config.DatabaseName); err != nil {
			return err
		}
	}

	if len(ch.config.MigrationsTable) == 0 {
		ch.config.MigrationsTable = DefaultMigrationsTable
	}

	if ch.config.MultiStatementMaxSize <= 0 {
		ch.config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	if len(ch.config.MigrationsTableEngine) == 0 {
		ch.config.MigrationsTableEngine = DefaultMigrationsTableEngine
	}

	return ch.ensureVersionTable()
}

func (ch *ClickHouse) Run(r io.Reader) error {
	if ch.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(r, multiStmtDelimiter, ch.config.MultiStatementMaxSize, func(m []byte) bool {
			tq := strings.TrimSpace(string(m))
			if tq == "" {
				return true
			}
			if _, e := ch.conn.Exec(string(m)); e != nil {
				err = database.Error{OrigErr: e, Err: "migration failed", Query: m}
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}

	migration, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if _, err := ch.conn.Exec(string(migration)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migration}
	}

	return nil
}
func (ch *ClickHouse) Version() (int, bool, error) {
	var (
		version int
		dirty   uint8
		query   = "SELECT version, dirty FROM `" + ch.config.MigrationsTable + "` ORDER BY sequence DESC LIMIT 1"
	)
	if err := ch.conn.QueryRow(query).Scan(&version, &dirty); err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty == 1, nil
}

func (ch *ClickHouse) HostAddrs() ([]string, error) {
	ch.addrsMux.Lock()
	defer ch.addrsMux.Unlock()
	if len(ch.addrs) != 0 {
		return ch.addrs, nil
	}

	hostAddrs := make(map[string]struct{})
	query := "SELECT DISTINCT host_address FROM system.clusters WHERE host_address NOT IN ['localhost', '127.0.0.1'] AND cluster = '" + ch.config.ClusterName + "'"
	rows, err := ch.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var hostAddr string
		if err := rows.Scan(&hostAddr); err != nil {
			return nil, err
		}
		hostAddrs[hostAddr] = struct{}{}
	}

	if len(hostAddrs) != 0 {
		// connect to other host and do the same thing
		for hostAddr := range hostAddrs {
			ch.url.Host = hostAddr
			conn, err := sql.Open("clickhouse", ch.url.String())
			if err != nil {
				return nil, err
			}
			rows, err := conn.Query(query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				var hostAddr string
				if err := rows.Scan(&hostAddr); err != nil {
					return nil, err
				}
				hostAddrs[hostAddr] = struct{}{}
			}
			break
		}
	}

	addrs := make([]string, 0, len(hostAddrs))
	for addr := range hostAddrs {
		addrs = append(addrs, addr)
	}
	ch.addrs = addrs
	return addrs, nil
}

func (ch *ClickHouse) SetVersion(version int, dirty bool) error {
	var (
		bool = func(v bool) uint8 {
			if v {
				return 1
			}
			return 0
		}
		tx, err = ch.conn.Begin()
	)
	if err != nil {
		return err
	}

	hostAddrs, err := ch.HostAddrs()
	if err != nil {
		return err
	}
	timeNow := time.Now().UnixNano()

	// insert into remote table(s)
	for _, hostAddr := range hostAddrs {
		query := fmt.Sprintf(
			"INSERT INTO FUNCTION remote('%s', %s, %s) VALUES (%d, %d, %d)",
			hostAddr,
			ch.config.DatabaseName,
			ch.config.MigrationsTable,
			version,
			bool(dirty),
			timeNow,
		)
		if _, err := tx.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	return tx.Commit()
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the ClickHouse type.
func (ch *ClickHouse) ensureVersionTable() (err error) {
	if err = ch.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := ch.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (
			version    Int64,
			dirty      UInt8,
			sequence   UInt64
		) Engine=%s`, ch.config.MigrationsTable, ch.config.ClusterName, ch.config.MigrationsTableEngine)

	if strings.HasSuffix(ch.config.MigrationsTableEngine, "Tree") {
		query = fmt.Sprintf(`%s ORDER BY sequence`, query)
	}

	if _, err := ch.conn.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (ch *ClickHouse) Drop() (err error) {
	query := "SHOW TABLES FROM " + ch.config.DatabaseName
	tables, err := ch.conn.Query(query)

	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}

		query = "DROP TABLE IF EXISTS " + ch.config.DatabaseName + "." + table

		if _, err := ch.conn.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (ch *ClickHouse) Lock() error {
	if !ch.isLocked.CAS(false, true) {
		return database.ErrLocked
	}

	return nil
}
func (ch *ClickHouse) Unlock() error {
	if !ch.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}

	return nil
}
func (ch *ClickHouse) Close() error { return ch.conn.Close() }
