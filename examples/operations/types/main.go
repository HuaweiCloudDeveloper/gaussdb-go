package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtype"
	"github.com/google/uuid"
)

type CustomJSON struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	fmt.Println("\uF4B3 Transaction Management Demo (gaussdb-go)")
	fmt.Println("============================================")
	conn, err := gaussdbgo.Connect(context.Background(), "gaussdb://gaussdb:Gaussdb@123@localhost:5433/postgres")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
        DROP TABLE IF EXISTS type_test_all;
        DROP TYPE IF EXISTS mood;

        CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok');

        CREATE TABLE type_test_all (
            id SERIAL PRIMARY KEY,
            int_val INTEGER,
            bigint_val BIGINT,
            numeric_val NUMERIC,
            text_val TEXT,
            bool_val BOOLEAN,
            date_val DATE,
            ts_val TIMESTAMP,
            bytea_val BYTEA,
            uuid_val UUID,
            jsonb_val JSONB,
            jsonb_obj JSONB,
            arr_val TEXT[],
            enum_val mood,
            interval_val INTERVAL,
            cidr_val CIDR,
            inet_val INET,
            float_val DOUBLE PRECISION,
            real_val REAL,
            smallint_val SMALLINT,
            char_val CHAR(3),
            varchar_val VARCHAR(50),
            time_val TIME,
            tstz_val TIMESTAMPTZ
        )
    `)
	if err != nil {
		log.Fatal("❌ Create table failed:", err)
	}

	sampleUUID := uuid.New()
	jsonObj := CustomJSON{Name: "bob", Age: 42}
	jsonBytes, _ := json.Marshal(jsonObj)
	jsonMap := map[string]interface{}{"x": 1, "y": "ok"}
	jsonMapBytes, _ := json.Marshal(jsonMap)

	_, err = conn.Exec(context.Background(), `
        INSERT INTO type_test_all (
            int_val, bigint_val, numeric_val, text_val, bool_val, date_val,
            ts_val, bytea_val, uuid_val, jsonb_val, jsonb_obj, arr_val,
            enum_val, interval_val, cidr_val, inet_val, float_val, real_val,
            smallint_val, char_val, varchar_val, time_val, tstz_val
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
            $13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23
        )
    `,
		int32(1), int64(999999999999),
		gaussdbtype.Numeric{Int: new(big.Int).SetUint64(uint64(12345)), Exp: -3, Valid: true}, // 12.345
		"Go + gaussdb", true, time.Now().AddDate(0, -1, 0),
		time.Now(), []byte("binary data"), sampleUUID,
		jsonBytes, jsonMapBytes, []string{"a", "b", "c"},
		"happy", "2 days 3 hours", "192.168.1.0/24", "10.1.2.3",
		3.1415, float32(2.71), int16(7), "abc", "This is varchar",
		time.Now(), time.Now(),
	)
	if err != nil {
		log.Fatal("❌ Insert failed:", err)
	}

	var (
		intVal         int32
		bigIntVal      int64
		numericVal     gaussdbtype.Numeric
		textVal        string
		boolVal        bool
		dateVal        time.Time
		tsVal          time.Time
		byteaVal       []byte
		uuidVal        gaussdbtype.UUID
		jsonbVal       []byte
		jsonbObjVal    []byte
		arrVal         []string
		enumVal        string
		intervalVal    gaussdbtype.Interval
		cidrIpNet      *net.IPNet
		inetIP         net.IP
		floatVal       float64
		realVal        float32
		smallintVal    int16
		charVal        string
		varcharVal     string
		timeVal        time.Time
		timestamptzVal time.Time
	)

	err = conn.QueryRow(context.Background(), `
        SELECT 
            int_val, bigint_val, numeric_val, text_val, bool_val, date_val,
            ts_val, bytea_val, uuid_val, jsonb_val, jsonb_obj, arr_val,
            enum_val, interval_val, cidr_val, inet_val,
            float_val, real_val, smallint_val, char_val, varchar_val,
            time_val, tstz_val
        FROM type_test_all LIMIT 1
    `).Scan(
		&intVal, &bigIntVal, &numericVal, &textVal, &boolVal, &dateVal,
		&tsVal, &byteaVal, &uuidVal, &jsonbVal, &jsonbObjVal, &arrVal,
		&enumVal, &intervalVal, &cidrIpNet, &inetIP,
		&floatVal, &realVal, &smallintVal, &charVal, &varcharVal,
		&timeVal, &timestamptzVal,
	)
	if err != nil {
		log.Fatal("❌ Scan failed:", err)
	}

	fmt.Println("📜 intVal:", intVal)
	fmt.Println("📜 bigIntVal:", bigIntVal)
	// 使用 gaussdbtype.Numeric 的 Float64 方法
	numF, _ := numericVal.Float64Value()
	fmt.Printf("📜 numericVal: %.5f\n", numF.Float64)
	fmt.Println("📜 textVal:", textVal)
	fmt.Println("📜 boolVal:", boolVal)
	fmt.Println("📜 dateVal:", dateVal)
	fmt.Println("📜 tsVal:", tsVal)
	fmt.Println("📜 byteaVal:", string(byteaVal))
	fmt.Println("📜 uuidVal:", uuidVal)
	fmt.Println("📜 jsonbVal:", string(jsonbVal))
	var parsed CustomJSON
	_ = json.Unmarshal(jsonbVal, &parsed)
	fmt.Printf("📜 Parsed JSON struct: %+v\n", parsed)
	var parsedMap2 map[string]interface{}
	_ = json.Unmarshal(jsonbObjVal, &parsedMap2)
	fmt.Printf("📜 Parsed JSON map: %+v\n", parsedMap2)
	fmt.Println("📜 arrVal:", arrVal)
	fmt.Println("📜 enumVal:", enumVal)
	fmt.Printf("📜 intervalVal: %d days, %d µs\n", intervalVal.Days, intervalVal.Microseconds)
	if cidrIpNet != nil {
		fmt.Println("📜 cidrVal:", cidrIpNet.String())
	}
	fmt.Println("📜 inetVal:", inetIP.String())
	fmt.Println("📜 floatVal:", floatVal)
	fmt.Println("📜 realVal:", realVal)
	fmt.Println("📜 smallintVal:", smallintVal)
	fmt.Println("📜 charVal:", charVal)
	fmt.Println("📜 varcharVal:", varcharVal)
	fmt.Println("📜 timeVal:", timeVal.Format("15:04:05"))
	fmt.Println("📜 timestamptzVal:", timestamptzVal)
	fmt.Println("\n🎉 Types examples completed successfully!")
}
