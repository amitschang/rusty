//! A few examples of parallel processing single arrays by chunk

use std::ops::{Add, AddAssign};
use std::sync::Arc;
use std::thread;

pub fn process_array(arr: &mut [f32]) {
    thread::scope(|scope| {
        for (_ix, chunk) in arr.chunks_mut(3).enumerate() {
            scope.spawn(move || {
                for i in chunk.iter_mut() {
                    *i += 1.0 + _ix as f32;
                }
            });
        }
    });
}

pub fn process_array_t<T>(arr: &mut [T])
where
    T: From<i8> + Add<Output = T> + AddAssign + Send,
{
    thread::scope(|scope| {
        for (_ix, chunk) in arr.chunks_mut(3).enumerate() {
            scope.spawn(move || {
                for i in chunk.iter_mut() {
                    *i += T::from(1) + T::from(_ix as i8);
                }
            });
        }
    });
}
