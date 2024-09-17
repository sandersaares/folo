/// Just combines a value and a reference count, for use in custom reference counting logic.
#[derive(Debug)]
pub struct WithRefCount<T> {
    value: T,
    ref_count: usize,
}

impl<T> WithRefCount<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            ref_count: 0,
        }
    }

    pub fn get(&self) -> &T {
        &self.value
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub fn inc_ref(&mut self) {
        self.ref_count += 1;
    }

    pub fn dec_ref(&mut self) {
        self.ref_count -= 1;
    }

    pub fn ref_count(&self) -> usize {
        self.ref_count
    }

    pub fn is_referenced(&self) -> bool {
        self.ref_count > 0
    }
}

impl<T> Default for WithRefCount<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::new(T::default())
    }
}
