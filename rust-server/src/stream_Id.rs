use std::fmt::{Display, Formatter};
use std::ops::Deref;

/// Let's see if u32 is enough. Newtype for nicer formatting.
#[derive(Copy, Clone)]
pub(crate) struct StreamId(pub u32);

impl Display for StreamId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

impl Deref for StreamId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
