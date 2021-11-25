use std::any::Any;
use std::collections::HashMap;

use serde::ser::{SerializeTuple, Serializer};

/// replaces the value with all stars
/// use by adding: #[serde(serialize_with = "serialize_masked")] to field
#[allow(dead_code)]
pub fn serialize_masked<S>(_: &dyn Any, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str("**********")
}

/// replaces the value with all stars
/// use by adding: #[serde(serialize_with = "serialize_masked")] to field
#[allow(dead_code)]
pub fn serialize_map_keys<S>(map: &HashMap<String, String>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut tup = s.serialize_tuple(map.len())?;
    for key in map.keys() {
        tup.serialize_element(key)?;
    }
    tup.end()
}

#[allow(dead_code)]
pub fn default_true() -> bool {
    true
}

#[allow(dead_code)]
pub fn default_false() -> bool {
    false
}
