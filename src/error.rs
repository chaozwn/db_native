use napi::Error;

pub type DbNativeResult<T> = napi::Result<T>;

pub fn into_napi_error<E>(error: E) -> Error
where
    E: std::fmt::Display,
{
    Error::from_reason(error.to_string())
}

pub fn state_error(message: impl Into<String>) -> Error {
    Error::from_reason(message.into())
}
