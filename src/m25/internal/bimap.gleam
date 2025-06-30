import gleam/dict

pub opaque type Bimap(a, b) {
  Bimap(from_key: dict.Dict(a, b), to_key: dict.Dict(b, a))
}

pub fn new() -> Bimap(a, b) {
  Bimap(dict.new(), dict.new())
}

pub fn insert(bimap: Bimap(a, b), key: a, value: b) -> Bimap(a, b) {
  let key_exists = dict.has_key(bimap.from_key, key)
  let value_exists = dict.has_key(bimap.to_key, value)

  case key_exists, value_exists {
    // The key and value already exist
    True, True -> {
      let cleaned =
        bimap
        |> delete_by_key(key)
        |> delete_by_value(value)

      Bimap(
        from_key: dict.insert(cleaned.from_key, key, value),
        to_key: dict.insert(cleaned.to_key, value, key),
      )
    }

    // The key exists by the value does not
    True, False -> {
      let cleaned = delete_by_key(bimap, key)
      Bimap(
        from_key: dict.insert(cleaned.from_key, key, value),
        to_key: dict.insert(cleaned.to_key, value, key),
      )
    }

    // The value exists by the key does not
    False, True -> {
      let cleaned = delete_by_value(bimap, value)
      Bimap(
        from_key: dict.insert(cleaned.from_key, key, value),
        to_key: dict.insert(cleaned.to_key, value, key),
      )
    }

    // Neither exists in the bimap already
    False, False ->
      Bimap(
        from_key: dict.insert(bimap.from_key, key, value),
        to_key: dict.insert(bimap.to_key, value, key),
      )
  }
}

pub fn get_by_key(bimap: Bimap(a, b), key: a) -> Result(b, Nil) {
  dict.get(bimap.from_key, key)
}

pub fn get_by_value(bimap: Bimap(a, b), value: b) -> Result(a, Nil) {
  dict.get(bimap.to_key, value)
}

pub fn delete_by_key(bimap: Bimap(a, b), key: a) -> Bimap(a, b) {
  case dict.get(bimap.from_key, key) {
    Error(_) -> bimap
    Ok(value) -> {
      let from_key = dict.delete(bimap.from_key, key)
      let to_key = dict.delete(bimap.to_key, value)
      Bimap(from_key, to_key)
    }
  }
}

pub fn delete_by_value(bimap: Bimap(a, b), value: b) -> Bimap(a, b) {
  case dict.get(bimap.to_key, value) {
    Error(_) -> bimap
    Ok(key) -> {
      let from_key = dict.delete(bimap.from_key, key)
      let to_key = dict.delete(bimap.to_key, value)
      Bimap(from_key, to_key)
    }
  }
}

pub fn size(bimap: Bimap(a, b)) -> Int {
  dict.size(bimap.from_key)
}

pub fn to_list(bimap: Bimap(a, b)) -> List(#(a, b)) {
  dict.to_list(bimap.from_key)
}
