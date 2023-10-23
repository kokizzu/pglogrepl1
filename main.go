package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kokizzu/gotro/L"
	"github.com/kokizzu/gotro/T"
	"github.com/kokizzu/lexid"
)

func main() {
	ctx := context.Background()
	const dsnRep = `postgres://pglogrepl:secret@127.0.0.1/pglogrepl?replication=database`
	const dsnNormal = `postgres://pglogrepl:secret@127.0.0.1/pglogrepl`

	conn, err := pgconn.Connect(ctx, dsnRep)
	L.PanicIf(err, `pgconn.Connect: `+dsnRep)

	ident, err := pglogrepl.IdentifySystem(ctx, conn)
	L.PanicIf(err, `pglogrepl.IdentifySystem`)

	slotName := `slot_` + T.Filename()
	tempOpt := pglogrepl.CreateReplicationSlotOptions{Temporary: true}
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", tempOpt)
	L.PanicIf(err, `pglogrepl.CreateReplicationSlot`)

	const pubName = `all_table_changes`
	const sqlDropPub = `DROP PUBLICATION IF EXISTS %s`
	read := conn.Exec(ctx, fmt.Sprintf(sqlDropPub, pubName))
	_, err = read.ReadAll()
	L.PanicIf(err, `conn.Exec: `+sqlDropPub)

	const sqlCreatePub = `CREATE PUBLICATION %s FOR ALL TABLES` // cannot if not exists '__') so need to do this on single migration
	read = conn.Exec(ctx, fmt.Sprintf(sqlCreatePub, pubName))
	_, err = read.ReadAll()
	L.PanicIf(err, `conn.Exec: `+sqlCreatePub)

	pluginArguments := []string{
		"proto_version '2'", // or `"pretty-print" true`
		fmt.Sprintf("publication_names '%s'", pubName),
		"messages 'true'",
		"streaming 'true'",
	}
	replOpts := pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}

	clientXLogPos := ident.XLogPos
	err = pglogrepl.StartReplication(ctx, conn, slotName, clientXLogPos, replOpts)
	L.PanicIf(err, `pglogrepl.StartReplication`)

	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
	typeMap := pgtype.NewMap()

	// whenever we get StreamStartMessage we set inStream to true and then pass it to DecodeV2 function
	// on StreamStopMessage we set it back to false
	inStream := false

	go runExampleInsertUpdate(dsnNormal)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParseXLogData failed:", err)
			}

			log.Printf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime)
			processV2(xld.WALData, relationsV2, typeMap, &inStream)

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}

func runExampleInsertUpdate(dsn string) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	L.PanicIf(err, `pgxpool.New: `+dsn)

	for x := 0; x < 3; x++ {

		time.Sleep(time.Second)
		const sqlInsert = `INSERT INTO foo(name) VALUES($1) RETURNING id`
		row := pool.QueryRow(ctx, sqlInsert, lexid.ID())
		L.PanicIf(err, `pool.Exec`)
		lastId := uint64(0)
		err := row.Scan(&lastId)
		L.PanicIf(err, `row.Scan`)

		time.Sleep(time.Second)

		const sqlUpdate = `UPDATE foo SET name=$1 WHERE id=$2`
		_, err = pool.Exec(ctx, sqlUpdate, lexid.ID(), lastId)
		L.PanicIf(err, `pool.Exec`)

	}
}

func processV2(walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, inStream *bool) {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		log.Fatalf("Parse logical replication message: %s", err)
	}
	log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessageV2:
		rel, values := decodeInsert(relations, logicalMsg, typeMap)
		log.Printf("insert for xid %d\n", logicalMsg.Xid)
		log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

	case *pglogrepl.UpdateMessageV2:
		rel, oldV, newV := decodeUpdate(relations, logicalMsg, typeMap)
		log.Printf("update for xid %d\n", logicalMsg.Xid)
		log.Printf("UPDATE %s.%s: %v TO %v", rel.Namespace, rel.RelationName, oldV, newV)
		// ...
	case *pglogrepl.DeleteMessageV2:
		log.Printf("delete for xid %d\n", logicalMsg.Xid)
		// ...
	case *pglogrepl.TruncateMessageV2:
		log.Printf("truncate for xid %d\n", logicalMsg.Xid)
		// ...

	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessageV2:
		log.Printf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		log.Printf("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
}

func decodeUpdate(relations map[uint32]*pglogrepl.RelationMessageV2, logicalMsg *pglogrepl.UpdateMessageV2, typeMap *pgtype.Map) (*pglogrepl.RelationMessageV2, map[string]any, map[string]any) {
	rel, ok := relations[logicalMsg.RelationID]
	if !ok {
		log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
	}
	oldValues := map[string]any{}
	if logicalMsg.OldTuple != nil {
		for idx, col := range logicalMsg.OldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				oldValues[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST oldValues are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				oldValues[colName] = val
			}
		}
	}

	newValues := map[string]any{}
	if logicalMsg.NewTuple != nil {
		for idx, col := range logicalMsg.NewTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				newValues[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST oldValues are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				newValues[colName] = val
			}
		}
	}

	return rel, oldValues, newValues
}

func decodeInsert(relations map[uint32]*pglogrepl.RelationMessageV2, logicalMsg *pglogrepl.InsertMessageV2, typeMap *pgtype.Map) (*pglogrepl.RelationMessageV2, map[string]interface{}) {
	rel, ok := relations[logicalMsg.RelationID]
	if !ok {
		log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
	}
	values := map[string]interface{}{}
	for idx, col := range logicalMsg.Tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				log.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		}
	}

	return rel, values
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
