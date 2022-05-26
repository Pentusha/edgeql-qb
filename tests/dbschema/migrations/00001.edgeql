CREATE MIGRATION m13n3l3xj2fzmqa5dqaoe32wl3bo3djbbxuou7d2vjocoeotgaj7gq
    ONTO initial
{
  CREATE TYPE default::A {
      CREATE ANNOTATION std::title := 'A';
      CREATE PROPERTY p_bigint -> std::bigint;
      CREATE PROPERTY p_bool -> std::bool {
          CREATE ANNOTATION std::title := 'single bool';
      };
      CREATE PROPERTY p_bytes -> std::bytes;
      CREATE PROPERTY p_datetime -> std::datetime;
      CREATE PROPERTY p_decimal -> std::decimal;
      CREATE PROPERTY p_duration -> std::duration;
      CREATE PROPERTY p_float32 -> std::float32;
      CREATE PROPERTY p_float64 -> std::float64;
      CREATE PROPERTY p_int16 -> std::int16;
      CREATE PROPERTY p_int32 -> std::int32;
      CREATE PROPERTY p_int64 -> std::int64;
      CREATE PROPERTY p_json -> std::json;
      CREATE PROPERTY p_local_date -> cal::local_date;
      CREATE PROPERTY p_local_datetime -> cal::local_datetime;
      CREATE PROPERTY p_local_time -> cal::local_time;
      CREATE PROPERTY p_str -> std::str;
  };
};
