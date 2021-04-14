use thiserror::Error;

#[derive(Debug, Error)]
pub enum BarnError {
    #[error("invalid resource, config validation failed")]
    InvalidResourceError,

    #[error("invalid attribute value given for indexing")]
    InvalidAttributeValueError,

    #[error("could not serialize the given resource")]
    SerializationError,

    #[error("could not deserialize the given resource")]
    DeSerializationError,

    #[error("could not open the environment")]
    EnvOpenError,

    #[error("invalid DB configuration")]
    DbConfigError,

    #[error("failed to commit transaction")]
    TxCommitError,

    #[error("failed to begin a new transaction")]
    TxBeginError,

    #[error("failed to write data")]
    TxWriteError,

    #[error("failed to read data")]
    TxReadError,

    #[error("invalid resource data error")]
    InvalidResourceDataError,

    #[error("resource not found")]
    ResourceNotFoundError,

    #[error("unknown resource name")]
    UnknownResourceName,

    #[error("unsupported index value type")]
    UnsupportedIndexValueType,

    #[error("bad search filter")]
    BadSearchFilter
}
