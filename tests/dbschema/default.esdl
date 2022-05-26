module default {

type A {
    annotation title := 'A';

    property p_bool -> bool {
        annotation title := 'single bool';
    }
    property p_str -> str;
    property p_datetime -> datetime;
    property p_local_datetime -> cal::local_datetime;
    property p_local_date -> cal::local_date;
    property p_local_time -> cal::local_time;
    property p_duration -> duration;
    property p_int16 -> int16;
    property p_int32 -> int32;
    property p_int64 -> int64;
    property p_float32 -> float32;
    property p_float64 -> float64;
    property p_bigint -> bigint;
    property p_decimal -> decimal;
    property p_json -> json;
    property p_bytes -> bytes;
}

type Nested3 {
    property name -> str;
}

type Nested2 {
    property name -> str;
    link nested3 -> Nested3;
}

type Nested1 {
    property name -> str;
    link nested2 -> Nested2;
}

}
